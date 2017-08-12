
#coding: utf-8

import gzip
import io
import os
import json
import shutil

import logging

from cachecontrol import CacheControl, CacheControlAdapter
from cachecontrol.caches.file_cache import \
	FileCache, _secure_open_write, url_to_file_path
from cachecontrol.serialize import Serializer, HTTPResponse, text_type
from cachecontrol.controller import CacheController, logger

from requests.structures import CaseInsensitiveDict


class ICECATFileCache(FileCache):

    host = 'data.icecat.biz/'
    root = 'export/freexml/'
    chunk_size = 1024 * 1024

    hostlen = len(host)
    rootlen = len(root)

    def __init__(self, directory, forever=False, filemode=0o0600,
                 dirmode=0o0700, use_dir_lock=None, lock_class=None, **kw):
        ret = super(ICECATFileCache, self).__init__(
            directory, forever=forever, filemode=filemode,
            dirmode=dirmode, use_dir_lock=use_dir_lock, lock_class=lock_class)
        for k, v in kw.iteritems():
            setattr(self, k, v)
        self.directorylen = len(directory)
        return ret

    def _fn(self, name):
        # NOTE: This method should not change as some may depend on it.
        #       See: https://github.com/ionrock/cachecontrol/issues/63
        if '://' in name:
            name = name[name.index('://') + 3:]
        if name.startswith(self.host):
            name = name[self.hostlen:]
        if name.startswith(self.root):
            name = name[self.rootlen:]
        name = os.path.join(self.directory, self.root, name)
        if not (name.endswith('.xml') or name.endswith('.xml.gz')):
			if self.with_index:
				name += self.with_index
        return name

    def get(self, key):
        name = self._fn(key)
        if not os.path.exists(name):
            return None
        if not os.path.exists(name+'.response'):
            return None

        data = None
        with open(name+'.response', 'rb') as fh:
            data = fh.read()
        if data:
            data = json.loads(data)

        return data

    def set(self, key, value):
        name = self._fn(key)

        # Make sure the directory exists
        try:
            os.makedirs(os.path.dirname(name), self.dirmode)
        except (IOError, OSError):
            pass

        with self.lock_class(name) as lock:
            # Write our actual file
            with _secure_open_write(lock.path, self.filemode) as fh:
                resp = value[u'response']
                inp = resp[u'body']
                headers = resp['headers']

                compress = headers.get('Content-Type') != 'application/x-gzip' \
                    and headers.get('Content-Encoding') != 'gzip'
                if compress:
                    with gzip.GzipFile(
                            filename=os.path.basename(name), fileobj=fh) as gz:
                        shutil.copyfileobj(inp, gz)
                    headers['Content-Encoding'] = 'gzip'
                    headers['Content-Length'] = str(fh.tell())
                else:
                    shutil.copyfileobj(inp, fh)

        with self.lock_class(name+'.response') as lock:
            # Write our actual file
            with _secure_open_write(lock.path, self.filemode) as fh:
                value = dict(value)
                value[u'response'][u'body'] = name[self.directorylen + 1:]
                fh.write(json.dumps(value, indent=4, separators=(',', ': ')))

    def delete(self, key):
        name = self._fn(key)
        if not self.forever:
            try:
                os.remove(name)
                os.remove(name+'.response')
            except FileNotFoundError:
                pass

    def url_to_file_path(self, key):
        return url_to_file_path(key, self)


class ICECATSerializer(Serializer):

    def __init__(self, cache):
        self.cache = cache

    def dumps(self, request, response, body=None):
        response_headers = CaseInsensitiveDict(response.headers)

        if body is None:
            body = response._fp
        else:
            body = io.BytesIO(body)
            response._fp = body

        data = {
            u"response": {
                u"body": body,
                u"headers": dict(
                    (text_type(k), text_type(v))
                    for k, v in response.headers.items()
                ),
                u"status": response.status,
                u"version": response.version,
                u"reason": text_type(response.reason),
                u"strict": response.strict,
                u"decode_content": response.decode_content,
            },
            u"vary": {
            },
        }

        # Construct our vary headers
        if u"vary" in response_headers:
            varied_headers = response_headers[u'vary'].split(',')
            for header in varied_headers:
                header = header.strip()
                header_value = request.headers.get(header, None)
                if header_value is not None:
                    header_value = text_type(header_value)
                data[u"vary"][header] = header_value

        return data

    def loads(self, request, data):
        # Short circuit if we've been given an empty set of data
        if not data:
            return

        return self.prepare_response(request, data)

    def prepare_response(self, request, cached):
        """Verify our vary headers match and construct a real urllib3
        HTTPResponse object.
        """
        # Special case the '*' Vary value as it means we cannot actually
        # determine if the cached response is suitable for this request.
        if "*" in cached.get("vary", {}):
            return

        # Ensure that the Vary headers for the cached response match our
        # request
        for header, value in cached.get("vary", {}).items():
            if request.headers.get(header, None) != value:
                return

        body_file = cached[u'response'].pop(u'body')
        body = open(os.path.join(self.cache.directory, body_file), 'rb')

        headers = CaseInsensitiveDict(data=cached['response']['headers'])
        if headers.get('transfer-encoding', '') == 'chunked':
            headers.pop('transfer-encoding')

        cached['response']['headers'] = headers

        return HTTPResponse(
            body=body,
            preload_content=False,
            **cached["response"]
        )


class ICECATCacheController(CacheController):
    """An interface to see if request should cached or not.
    """
    def __init__(self, cache=None, cache_etags=True, serializer=None,
                 status_codes=None, 
                 cache_always_save=True, 
                 cache_always_fetch=False,
                 cache_always_use=True):

        self.cache_always_save = cache_always_save
        self.cache_always_fetch = cache_always_fetch
        self.cache_always_use = cache_always_use

        ret = super(ICECATCacheController, self).__init__(
                    cache=cache, cache_etags=cache_etags, serializer=serializer,
                    status_codes=status_codes)
        return ret

    def cached_request(self, request):
        """
        Return a cached response if it exists in the cache, otherwise
        return False.
        """
        if self.cache_always_fetch:
            logger.debug('Fetching anyway')
            return False
        if not self.cache_always_use:
            return super(ICECATCacheController, self).cached_request(request)

        cache_url = self.cache_url(request.url)
        logger.debug('Looking up "%s" in the cache', cache_url)

        # Request allows serving from the cache, let's see if we find something
        cache_data = self.cache.get(cache_url)
        if cache_data is None:
            logger.debug('No cache entry available')
            return False

        # Check whether it can be deserialized
        resp = self.serializer.loads(request, cache_data)
        if not resp:
            logger.warning('Cache entry deserialization failed, entry ignored')
            return False

        return resp


    def cache_response(self, request, response, body=None,
                       status_codes=None):
        if not self.cache_always_save:
            super(ICECATCacheController, self).cache_response(
                request, response, body=body, status_codes=status_codes)
            return 

        cacheable_status_codes = status_codes or self.cacheable_status_codes

        if response.status in cacheable_status_codes:
            cache_url = self.cache_url(request.url)
            logger.debug('Caching by accepted status')
            self.cache.set(
                cache_url,
                self.serializer.dumps(request, response)
            )


def setup_logging():
	logger.setLevel(logging.DEBUG)
	handler = logging.StreamHandler()
	logger.addHandler(handler)


def ICECATCacheControl(
        session, directory=None, file_cache=None, 
        cache_always_save=True, 
        cache_always_fetch=False, 
        cache_always_use=True):

    class ICECATCacheControlAdapter(CacheControlAdapter):
      def __init__(self, cache=None,
                 cache_etags=True,
                 controller_class=None,
                 serializer=None,
                 heuristic=None,
                 cacheable_methods=None,
                 *args, **kw):
        super(ICECATCacheControlAdapter, self).__init__(
            *args, 
            cache=cache,
            cache_etags=cache_etags,
            controller_class=controller_class,
            serializer=serializer,
            heuristic=heuristic,
            cacheable_methods=cacheable_methods,
            **kw
        )
        self.controller = ICECATCacheController(
            cache,
            cache_etags=cache_etags,
            serializer=serializer,
            cache_always_save=cache_always_save,
            cache_always_fetch=cache_always_fetch,
            cache_always_use=cache_always_use,
        )

      def build_response(self, request, response, from_cache=False,
                       cacheable_methods=None):
        if not (cache_always_fetch or cache_always_save):
            return super(ICECATCacheControlAdapter, self).build_response(
                request, response, from_cache=from_cache,
                cacheable_methods=cacheable_methods)
        """
        Build a response by making a request or using the cache.

        This will end up calling send and returning a potentially
        cached response
        """
        cacheable = cacheable_methods or self.cacheable_methods
        if not from_cache and request.method in cacheable:
            # We always cache the 301 responses
            if response.status == 200:
                self.controller.cache_response(request, response)

        resp = super(ICECATCacheControlAdapter, self).build_response(
            request, response
        )

        return resp


    setup_logging()

    file_cache = file_cache or ICECATFileCache(directory)
    serializer = ICECATSerializer(file_cache)

    session = CacheControl(
        session,
        cache=file_cache,
        serializer=serializer,
        adapter_class=ICECATCacheControlAdapter,
    )

    return session
