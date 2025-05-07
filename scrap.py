import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import csv
from pymongo import MongoClient

from utils.debug_color import debug_print

# Configuration MongoDB
MONGO_URI = 'mongodb://localhost:27017/'
MONGO_DB = 'ipssi_webscraping'
MONGO_COLLECTION = 'Scrapy'

# Compteurs pour la console allégée
class ScrapingStats:
    def __init__(self):
        self.requests_total = 0
        self.requests_success = 0
        self.requests_failed = 0
        self.items_extracted = 0
        self.mongodb_updates = 0
        self.mongodb_errors = 0
        self.spiders_completed = 0
    
    def print_summary(self):
        debug_print("=== Résumé du scraping ===", "info")
        debug_print(f"Requêtes totales : {self.requests_total}", "info")
        debug_print(f"Requêtes réussies : {self.requests_success}", "success")
        debug_print(f"Requêtes échouées : {self.requests_failed}", "error")
        debug_print(f"Éléments extraits : {self.items_extracted}", "info")
        debug_print(f"Mises à jour MongoDB : {self.mongodb_updates}", "info")
        debug_print(f"Erreurs MongoDB : {self.mongodb_errors}", "warning")
        debug_print(f"Spiders complétés : {self.spiders_completed}", "success")
        debug_print("========================", "info")

# Instance globale pour les statistiques
scraping_stats = ScrapingStats()

# Classes d'items pour stocker les données extraites
class EntrepriseItem(scrapy.Item):
    # ... (code inchangé)
    numero_entreprise = scrapy.Field()
    generalites = scrapy.Field()
    fonctions = scrapy.Field()
    capacites_entrepreneuriales = scrapy.Field()
    qualites = scrapy.Field()
    autorisations = scrapy.Field()
    nace_2025 = scrapy.Field()
    nace_2008 = scrapy.Field()
    nace_2003 = scrapy.Field()
    donnees_financieres = scrapy.Field()
    liens_entites = scrapy.Field()
    liens_externes = scrapy.Field()
    publications = scrapy.Field()
    comptes_annuels = scrapy.Field()

# Spider 1: KBO Spider
class KboSpider(scrapy.Spider):
    name = 'kbo_spider'
    allowed_domains = ['kbopub.economie.fgov.be']
    
    # Désactiver la journalisation par défaut de Scrapy
    custom_settings = {
        'LOG_ENABLED': False,
        'ROBOTSTXT_OBEY': True
    }
    
    def __init__(self, *args, **kwargs):
        super(KboSpider, self).__init__(*args, **kwargs)
        self.numeros_entreprise = self.load_numeros_entreprise()
        debug_print(f"KBO Spider initialisé avec {len(self.numeros_entreprise)} entreprises", "info")
        
    def load_numeros_entreprise(self):
        numeros = []
        try:
            with open('enterprise.csv', 'r') as f:
                csv_reader = csv.reader(f)
                for row in csv_reader:
                    if row and len(row) > 0 and row[0].strip('"'):
                        numero = row[0].strip('"')
                        # Formater le numéro pour enlever les points
                        numero = numero.replace('.', '')
                        numeros.append(numero)
            debug_print(f"Chargé {len(numeros)} numéros d'entreprise", "success")
            return numeros
        except Exception as e:
            debug_print(f"Erreur lors du chargement des numéros d'entreprise: {e}", "error")
            return []
        
    def start_requests(self):
        for i, numero in enumerate(self.numeros_entreprise):
            url = f'https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html?ondernemingsnummer={numero}'
            if i % 10 == 0:  # Afficher seulement tous les 10 pour alléger
                debug_print(f"Requête KBO [{i+1}/{len(self.numeros_entreprise)}] pour {numero}", "fetch")
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                headers={'Accept-Language': 'fr-FR,fr;q=0.9'},  # Pour obtenir la version francophone
                meta={'numero_entreprise': numero},
                errback=self.errback_http
            )
            scraping_stats.requests_total += 1
    
    def errback_http(self, failure):
        # Appelé lorsqu'une erreur HTTP se produit
        request = failure.request
        numero_entreprise = request.meta['numero_entreprise']
        debug_print(f"Échec de la requête pour l'entreprise {numero_entreprise}: {failure.value}", "error")
        scraping_stats.requests_failed += 1
    
    def parse(self, response):
        # Traitement d'une réponse réussie
        scraping_stats.requests_success += 1
        
        numero_entreprise = response.meta['numero_entreprise']
        item = EntrepriseItem()
        item['numero_entreprise'] = numero_entreprise
        
        # Extraire les données (code de la fonction inchangé)
        item['generalites'] = self.extract_generalites(response)
        item['fonctions'] = self.extract_fonctions(response)
        item['capacites_entrepreneuriales'] = self.extract_capacites(response)
        item['qualites'] = self.extract_qualites(response)
        item['autorisations'] = self.extract_autorisations(response)
        item['nace_2025'] = self.extract_nace(response, '2025')
        item['nace_2008'] = self.extract_nace(response, '2008')
        item['nace_2003'] = self.extract_nace(response, '2003')
        item['donnees_financieres'] = self.extract_donnees_financieres(response)
        item['liens_entites'] = self.extract_liens_entites(response)
        item['liens_externes'] = self.extract_liens_externes(response)
        item['publications'] = []
        item['comptes_annuels'] = []
        
        debug_print(f"Données extraites pour l'entreprise {numero_entreprise}", "success")
        scraping_stats.items_extracted += 1
        
        yield item
    
    def extract_generalites(self, response):
        generalites = {}
        try:
            # (code inchangé)
            generalites_section = response.xpath('//div[contains(@class, "data-header")]')
            generalites['nom'] = generalites_section.xpath('.//h2/text()').get()
            generalites['statut'] = generalites_section.xpath('.//div[contains(text(), "Statut")]/following-sibling::div/text()').get()
            adresse_elements = response.xpath('//div[contains(@class, "address")]/text()').getall()
            generalites['adresse'] = ' '.join([elem.strip() for elem in adresse_elements if elem.strip()])
            generalites['forme_juridique'] = response.xpath('//th[contains(text(), "Forme juridique")]/following-sibling::td/text()').get()
            generalites['date_debut'] = response.xpath('//th[contains(text(), "Date de début")]/following-sibling::td/text()').get()
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des généralités: {e}", "error")
        return generalites
    
    # Les autres méthodes d'extraction restent inchangées mais on pourrait 
    # remplacer les self.logger.error par debug_print(..., "error")
    # Pour plus de lisibilité, je ne duplique pas toutes ces méthodes ici

# Spider 2: eJustice Spider
class EjusticeSpider(scrapy.Spider):
    name = 'ejustice'
    allowed_domains = ['ejustice.just.fgov.be']
    
    # Désactiver la journalisation par défaut de Scrapy et ignorer robots.txt
    custom_settings = {
        'LOG_ENABLED': False,
        'ROBOTSTXT_OBEY': False  # Désactive l'obéissance au robots.txt
    }
    
    # Reste du code similaire avec ajout des debug_print et stats...

# Spider 3: Consult Spider (NBB)
class ConsultSpider(scrapy.Spider):
    name = 'consult'
    allowed_domains = ['consult.cbso.nbb.be']
    
    custom_settings = {
        'LOG_ENABLED': False,
        'ROBOTSTXT_OBEY': True
    }
    
    # Reste du code similaire avec ajout des debug_print et stats...

# Pipeline MongoDB pour stocker les données
class MongoDBPipeline:
    def __init__(self):
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB]
        self.collection = self.db[MONGO_COLLECTION]
        debug_print(f"Pipeline MongoDB initialisée - Collection: {MONGO_COLLECTION}", "info")
    
    def process_item(self, item, spider):
        if spider.name == 'kbo_spider':
            try:
                self.collection.update_one(
                    {'numero_entreprise': item['numero_entreprise']},
                    {'$set': dict(item)},
                    upsert=True
                )
                debug_print(f"Entreprise {item['numero_entreprise']} mise à jour dans MongoDB", "success")
                scraping_stats.mongodb_updates += 1
            except Exception as e:
                debug_print(f"Erreur MongoDB: {e}", "error")
                scraping_stats.mongodb_errors += 1
        return item
    
    def close_spider(self, spider):
        self.client.close()
        debug_print(f"Spider '{spider.name}' terminé", "info")
        scraping_stats.spiders_completed += 1
        
        # Si c'était le dernier spider, afficher le résumé
        if scraping_stats.spiders_completed == 3:  # Nombre total de spiders
            scraping_stats.print_summary()

# Configuration du crawler avec console allégée
def configure_crawler():
    settings = get_project_settings()
    settings.set('ITEM_PIPELINES', {
        '__main__.MongoDBPipeline': 300,
    })
    settings.set('USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    settings.set('LOG_ENABLED', False)  # Désactiver les logs Scrapy par défaut
    settings.set('DOWNLOAD_DELAY', 1)
    return settings

# Fonction principale pour exécuter les spiders
def main():
    debug_print("Démarrage du scraping des entreprises belges", "info")
    debug_print("Configuration du crawler...", "debug")
    
    # Configurer et démarrer le crawler
    settings = configure_crawler()
    process = CrawlerProcess(settings)
    
    # Ajouter les spiders au processus
    debug_print("Ajout des spiders au processus...", "info")
    process.crawl(KboSpider)
    process.crawl(EjusticeSpider)
    process.crawl(ConsultSpider)
    
    # Démarrer le crawling
    debug_print("Démarrage du crawling...", "info")
    process.start()
    
    debug_print("Processus de scraping terminé", "success")

if __name__ == "__main__":
    main()