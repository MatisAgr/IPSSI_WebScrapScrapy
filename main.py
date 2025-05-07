import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from pymongo import MongoClient

from utils.debug_color import debug_print
from spiders import KboSpider, EjusticeSpider, ConsultSpider, scraping_stats

# Configuration MongoDB
MONGO_URI = 'mongodb://localhost:27017/'
MONGO_DB = 'ipssi_webscraping'
MONGO_COLLECTION = 'Scrapy'

# Pipeline MongoDB pour stocker les données
class MongoDBPipeline:
    def __init__(self):
        try:
            self.client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)  # 5 secondes timeout
            # Test de connexion
            self.client.server_info()  # Va lever une exception si la connexion échoue
            self.db = self.client[MONGO_DB]
            self.collection = self.db[MONGO_COLLECTION]
            debug_print(f"Pipeline MongoDB initialisée - Collection: {MONGO_COLLECTION}", "info")
        except Exception as e:
            debug_print(f"ERREUR CRITIQUE: Impossible de se connecter à MongoDB: {e}", "error")

    def process_item(self, item, spider):
        try:
            if spider.name == 'kbo_spider':
                self.collection.update_one(
                    {'numero_entreprise': item['numero_entreprise']},
                    {'$set': dict(item)},
                    upsert=True
                )
                debug_print(f"Entreprise {item['numero_entreprise']} mise à jour dans MongoDB", "success")
                scraping_stats.mongodb_updates += 1
            elif spider.name == 'ejustice':
                # Pour les autres spiders, mettre à jour des champs spécifiques
                self.collection.update_one(
                    {'numero_entreprise': item.get('numero_entreprise')},
                    {'$set': {'publications': item.get('publications', [])}},
                    upsert=True
                )
                debug_print(f"Publications mises à jour pour {item.get('numero_entreprise')}", "success")
                scraping_stats.mongodb_updates += 1
            elif spider.name == 'consult':
                self.collection.update_one(
                    {'numero_entreprise': item.get('numero_entreprise')},
                    {'$set': {'comptes_annuels': item.get('comptes_annuels', [])}},
                    upsert=True
                )
                debug_print(f"Comptes annuels mis à jour pour {item.get('numero_entreprise')}", "success")
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
        if scraping_stats.spiders_completed == 1:  # Ajuster selon le nombre de spiders actifs
            scraping_stats.print_summary()

# Configuration du crawler avec console allégée
def configure_crawler():
    settings = get_project_settings()
    settings.set('ITEM_PIPELINES', {
        'main.MongoDBPipeline': 300,
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
    # Décommenter pour activer les autres spiders
    # process.crawl(EjusticeSpider)
    # process.crawl(ConsultSpider)
    
    # Démarrer le crawling
    debug_print("Démarrage du crawling...", "info")
    process.start()
    
    debug_print("Processus de scraping terminé", "success")

if __name__ == "__main__":
    main()