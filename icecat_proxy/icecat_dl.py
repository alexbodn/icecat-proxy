# icecat download

import requests
import os
import gzip
from lxml import objectify, etree


class IceCatLoader(object):

	host = 'https://data.icecat.biz/'
	root = 'export/freexml/'
	_index = '/icecat-free.xml'

	refs = 'refs/%s.xml'
	refs_gz = 'refs/%s.xml.gz'

	chunk_size = 1024 * 1024

	def __init__(self, config):

		self.user = config['user']
		self.passwd = config['passwd']
		self.cache = config['cache']

	def dl_xml(
			self, path, session=None, with_index='', force=False, root=None, 
			parser=objectify, 
		):

		path = (self.root if root is None else root) + path

		dl_file = os.path.join(self.cache, path)
		if not (dl_file.endswith('.xml') or dl_file.endswith('.xml.gz')):
			if with_index:
				dl_file += with_index

		if not os.path.exists(dl_file):
			force = True

		objectified_xml = None
		if not force:
			try:
				objectified_xml = parser.parse(dl_file)
			except:
				pass
		if objectified_xml is None:
			if session is None:
				session = requests.Session()
			session.auth = (self.user, self.passwd)
			res = session.get(self.host + path)
			status_code = res.status_code

			if 200 <= status_code < 299:
				filedir = os.path.dirname(dl_file)
				if not os.path.exists(filedir):
					os.makedirs(filedir)
				with open(dl_file, 'wb') as of:
					for chunk in res.iter_content(chunk_size=self.chunk_size):
						if chunk:
							of.write(chunk)

			objectified_xml = parser.parse(dl_file)

		if hasattr(objectified_xml, 'xpath'):
			for path in objectified_xml.xpath('//file/@path'):
				self.dl_xml(path=path, session=session, force=force, root='')

		return objectified_xml

	def products(self, lang, force=False, with_index=''):
		return self.dl_xml(path=lang, with_index=with_index or self._index)

	def campaigns(self, force=False):
		return self.dl_xml(path=self.refs % 'CampaignsList', force=force)

	def categories(self, force=False):
		return self.dl_xml(path=self.refs_gz % 'CategoriesList', force=force)

	def categoriesfeatures(self, force=False):
		return self.dl_xml(
			path=self.refs_gz % 'CategoryFeaturesList', force=force, 
			parser=categoriesfeatures_parser(),
		)

	def featuregroups(self, force=False):
		return self.dl_xml(
			path=self.refs_gz % 'CategoryFeaturesList', force=force, 
			parser=featuregroups_parser(),
		)

	def features(self, force=False):
		return self.dl_xml(path=self.refs_gz % 'FeaturesList', force=force)

	def featurevalues(self, force=False):
		return self.dl_xml(path=self.refs_gz % 'FeatureValuesVocabularyList', force=force)

	def languages(self, force=False):
		return self.dl_xml(path=self.refs_gz % 'LanguageList', force=force)

	def measures(self, force=False):
		return self.dl_xml(path=self.refs_gz % 'MeasuresList', force=force)

	def relations(self, force=False):
		return self.dl_xml(path=self.refs % 'RelationsList', force=force)

	def supplierproductfamilies(self, force=False):
		return self.dl_xml(path=self.refs_gz % 'SupplierProductFamiliesListRequest', force=force)

	def suppliers(self, force=False):
		return self.dl_xml(path=self.refs_gz % 'SuppliersList', force=force)


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
					categoryfeaturegroup = etree.SubElement(
						category, elem.tag, attrib=elem.attrib)
				if elem.tag=='FeatureGroup':
					featuregroup = etree.SubElement(
						categoryfeaturegroup, elem.tag, attrib=elem.attrib)

			if event == 'end':
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


def langids(force=False):
	expression = '//LanguageList//Language[@ID and @ShortCode]'
	languages = loader.languages(force=force).xpath(expression)
	return dict([
		(lang.get('ShortCode'), lang.get('ID')) for lang in languages
	])

def measures(force=False):
	expression = '//MeasuresList//Measure[@ID and Sign]'
	measures = loader.measures(force=force).xpath(expression)
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

	#loader.products(lang=lang)
	#loader.categories()
	f = loader.categoriesfeatures()
	#f = loader.featuregroups()
	#loader.features()
	#spf = loader.supplierproductfamilies()
	#f = loader.featurevalues()
	#f = loader.features()
	f.write(
		'output.xml', xml_declaration=True, pretty_print=True, encoding='utf-8')
	#langids()

