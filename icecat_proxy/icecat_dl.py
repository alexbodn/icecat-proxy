# icecat download

import requests

from _filecache import ICECATCacheControl, ICECATFileCache, url_to_file_path

import shutil
import os
import gzip
from lxml import objectify, etree


class IceCatLoader(object):

	host = 'data.icecat.biz/'
	root = 'export/freexml/'

	_index = '/icecat-free.xml'

	refs = 'refs/%s.xml'
	refs_gz = 'refs/%s.xml.gz'

	def __init__(self, config):

		self.user = config['user']
		self.passwd = config['passwd']
		cache = config['cache']
		self.file_cache = ICECATFileCache(cache+'1', with_index=self._index)

	def dl_xml(self, path, session=None, root=None, parser=objectify, **kw):

		path = (self.root if root is None else root) + path
		url = 'https://' + self.host + path

		if session is None:
			session = requests.Session()
			session.auth = (self.user, self.passwd)
			session = ICECATCacheControl(
				session,
				file_cache=self.file_cache,
				**kw
			)

		with session.get(url, stream=True) as res:
			pass

		dl_file = self.file_cache.url_to_file_path(url)
		objectified_xml = parser.parse(dl_file)

		if hasattr(objectified_xml, 'xpath'):
			for path in objectified_xml.xpath('//file/@path'):
				self.dl_xml(path=path, session=session, root='', **kw)

		return objectified_xml

	def products(self, lang, **kw):
		return self.dl_xml(path=lang, **kw)

	def campaigns(self, **kw):
		return self.dl_xml(path=self.refs % 'CampaignsList', **kw)

	def categories(self, **kw):
		return self.dl_xml(path=self.refs_gz % 'CategoriesList', **kw)

	def categoriesfeatures(self, **kw):
		return self.dl_xml(
			path=self.refs_gz % 'CategoryFeaturesList', 
			parser=categoriesfeatures_parser(), 
			**kw
		)

	def featuregroups(self, **kw):
		return self.dl_xml(
			path=self.refs_gz % 'CategoryFeaturesList', 
			parser=featuregroups_parser(), 
			**kw
		)

	def features(self, **kw):
		return self.dl_xml(path=self.refs_gz % 'FeaturesList', **kw)

	def featurevalues(self, **kw):
		return self.dl_xml(
			path=self.refs_gz % 'FeatureValuesVocabularyList', **kw)

	def languages(self, **kw):
		return self.dl_xml(path=self.refs_gz % 'LanguageList', **kw)

	def measures(self, **kw):
		return self.dl_xml(path=self.refs_gz % 'MeasuresList', **kw)

	def relations(self, **kw):
		return self.dl_xml(path=self.refs % 'RelationsList', **kw)

	def supplierproductfamilies(self, **kw):
		return self.dl_xml(
			path=self.refs_gz % 'SupplierProductFamiliesListRequest', **kw)

	def suppliers(self, **kw):
		return self.dl_xml(path=self.refs_gz % 'SuppliersList', **kw)


class categoriesfeatures_parser(etree.XMLParser):

	def __init__(self):
		pass

	def parse(self, source):
		# get an iterable
		context = etree.iterparse(
			gzip.GzipFile(source), events=("start", "end", ))
		doc = None
		root = None

		for event, elem in context:
			if event == 'start':
				if elem.tag=='CategoryFeaturesList':
					# Create the root element
					doc_root = etree.Element(elem.tag, attrib=elem.attrib)
					# Make a new document tree
					doc = etree.ElementTree(doc_root)
				if elem.tag=='Category':
					category = etree.SubElement(
						doc_root, elem.tag, attrib=elem.attrib)
				if elem.tag=='CategoryFeatureGroup':
					if not category is None:
						categoryfeaturegroup = etree.SubElement(
							category, elem.tag, attrib=elem.attrib)
				if elem.tag=='FeatureGroup':
					if not categoryfeaturegroup is None:
						featuregroup = etree.SubElement(
							categoryfeaturegroup, elem.tag, attrib=elem.attrib)

			if event == 'end':
				if elem.tag=='Category':
					category = None
				if elem.tag=='CategoryFeatureGroup':
					categoryfeaturegroup = None
				elem.clear()
				if root is None:
					root = elem
				root.clear()

		return doc


class featuregroups_parser(etree.XMLParser):

	def __init__(self):
		self.groups = dict()
		self.groupnames = dict()
		self.categoryfeaturegroups = dict()

	def parse(self, source):
		# get an iterable
		context = etree.iterparse(
			gzip.GzipFile(source), events=("start", "end", ))
		doc = None
		root = None

		for event, elem in context:
			if event == 'start':
				if elem.tag=='CategoryFeaturesList':
					# Create the root element
					doc_root = etree.Element('FeatureGroupList')
					# Make a new document tree
					doc = etree.ElementTree(doc_root)
				if elem.tag=='FeatureGroup':
					ID = elem.get('ID')
					if ID not in self.groups:
						featuregroup = etree.SubElement(
							doc_root, elem.tag, attrib=elem.attrib)
						self.groups[ID] = featuregroup
						self.groupnames[ID] = set()
						etree.SubElement(featuregroup, 'Names')
						etree.SubElement(featuregroup, 'Features')
						parent = elem.getparent()
						parentid = None if parent is None else parent.get('ID')
						self.categoryfeaturegroups[parentid] = featuregroup
				if elem.tag=='Name':
					parent = elem.getparent()
					featuregroup = parentid = None
					if parent is not None and parent.tag == 'FeatureGroup':
						parentid = parent.get('ID')
						featuregroup = self.groups.get(parentid)
					if featuregroup is not None:
						names = featuregroup.find('Names')
						ID = elem.get('ID')
						if ID not in self.groupnames.get(parentid):
							if names is not None:
								etree.SubElement(
									names, elem.tag, attrib=elem.attrib)
								self.groupnames[parentid].add(ID)
				if elem.tag=='Feature':
					groupid = elem.get('CategoryFeatureGroup_ID')
					featuregroup = self.categoryfeaturegroups.get(groupid)
					if featuregroup is not None:
						features = featuregroup.find('Features')
						if features is not None:
							etree.SubElement(
								features, elem.tag, attrib=elem.attrib)

			if event == 'end':
				if elem.tag in ('Feature', 'Name'):
					continue
				elem.clear()
				if root is None:
					root = elem
				root.clear()

		return doc


def langids(loader, **kw):
	expression = '//LanguageList//Language[@ID and @ShortCode]'
	languages = loader.languages(**kw).xpath(expression)
	return dict([
		(lang.get('ShortCode'), lang.get('ID')) for lang in languages
	])

def measures(loader, **kw):
	expression = '//MeasuresList//Measure[@ID and Sign]'
	measures = loader.measures(**kw).xpath(expression)
	return dict([
		(measure.find('Sign').text, measure.get('ID')) for measure in measures
	])

if __name__ == '__main__':

	cache = 'data'
	cache = os.path.join(os.path.abspath(os.path.dirname(__file__)), cache)

	loader = IceCatLoader(dict(
		user='alexbodn', 
		passwd='tkfx-1bodnari', 
		cache=cache, 
	))

	lang = 'HE'
	lang = 'INT'

	#f = loader.products(lang=lang, cache_always_save=True)
	#f = loader.categories()
	#f = loader.categoriesfeatures()
	#f = loader.featuregroups(cache_always_use=True)
	#f = loader.features()
	f = loader.languages(cache_always_fetch=True)
	#f = loader.relations(cache_always_fetch=True)
	#f = loader.supplierproductfamilies()
	#f = loader.featurevalues()
	#f = loader.features()
	f.write(
		'output.xml', xml_declaration=True, pretty_print=True, encoding='utf-8')
	#langids(loader)

