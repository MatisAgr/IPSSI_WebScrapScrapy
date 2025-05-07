import scrapy
import csv
from utils.debug_color import debug_print
from items import EntrepriseItem

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
            with open('enterprise_cropped.csv', 'r') as f:
                csv_reader = csv.reader(f)
                next(csv_reader)  # Ignorer l'en-tête
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
            # S'assurer que le format est correct (10 chiffres sans points)
            numero_clean = numero.replace('.', '')
            
            url = f'https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html?ondernemingsnummer={numero_clean}'
            
            if i % 10 == 0:  # Afficher seulement tous les 10 pour alléger
                debug_print(f"Requête KBO [{i+1}/{len(self.numeros_entreprise)}] pour {numero_clean}", "fetch")
            
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                headers={'Accept-Language': 'fr-FR,fr;q=0.9'},  # Pour obtenir la version francophone
                meta={'numero_entreprise': numero_clean},
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
        
        # Vérifier que le numéro d'entreprise est valide
        if not numero_entreprise or numero_entreprise == "EnterpriseNumber":
            debug_print(f"Numéro d'entreprise invalide: {numero_entreprise}", "error")
            return
        
        # Analyser la structure du HTML pour le débogage
        if scraping_stats.requests_success <= 2:
            self.analyze_page_structure(response)
        
        # Créer un nouvel item pour cette entreprise
        item = EntrepriseItem()
        item['numero_entreprise'] = numero_entreprise
        
        # Extraire les données
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
        
        # Ces champs seront remplis par d'autres spiders
        item['publications'] = []
        item['comptes_annuels'] = []
        
        if item['generalites']:
            debug_print(f"Données extraites pour l'entreprise {numero_entreprise}", "success")
            scraping_stats.items_extracted += 1
            yield item
        else:
            debug_print(f"Aucune donnée valide extraite pour {numero_entreprise}", "warning")
    
    def analyze_page_structure(self, response):
        """Analyser la structure de la page pour comprendre le HTML"""
        try:
            # Extraire les balises principales
            all_div_classes = response.xpath('//div/@class').getall()
            all_headers = response.xpath('//h1|//h2|//h3').getall()[:5]  # les 5 premiers en-têtes
            
            debug_print("=== ANALYSE DE STRUCTURE DE PAGE ===", "info")
            debug_print(f"URL: {response.url}", "info")
            debug_print(f"Classes DIV principales: {all_div_classes[:10]}", "debug")
            debug_print(f"En-têtes principaux: {all_headers}", "debug")
            
            # Enregistrer le HTML pour analyse
            with open(f"debug_page_{response.meta['numero_entreprise']}.html", 'w', encoding='utf-8') as f:
                f.write(response.text)
            debug_print(f"HTML enregistré dans debug_page_{response.meta['numero_entreprise']}.html", "info")
        except Exception as e:
            debug_print(f"Erreur lors de l'analyse de la structure: {e}", "error")
    
    # Méthodes d'extraction des données
    def extract_capacites(self, response):
        capacites = []
        try:
            capacites_section = response.xpath('//div[contains(., "Capacités entrepreneuriales")]/following-sibling::div[1]')
            for row in capacites_section.xpath('.//tr'):
                try:
                    capacite = {
                        'denomination': row.xpath('./td[1]//text()').get(),
                        'categorie': row.xpath('./td[2]//text()').get(),
                        'date_debut': row.xpath('./td[3]//text()').get()
                    }
                    if capacite['denomination']:
                        capacites.append(capacite)
                except Exception as e:
                    debug_print(f"Erreur sur une capacité: {e}", "debug")
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des capacités: {e}", "debug")
        return capacites
    
    def extract_qualites(self, response):
        qualites = []
        try:
            qualites_section = response.xpath('//div[contains(., "Qualités")]/following-sibling::div[1]')
            for row in qualites_section.xpath('.//tr'):
                try:
                    qualite = {
                        'denomination': row.xpath('./td[1]//text()').get(),
                        'date_debut': row.xpath('./td[2]//text()').get()
                    }
                    if qualite['denomination']:
                        qualites.append(qualite)
                except Exception as e:
                    debug_print(f"Erreur sur une qualité: {e}", "debug")
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des qualités: {e}", "debug")
        return qualites
    
    # Autres méthodes d'extraction (autorisations, NACE, données financières, etc.)
    def extract_autorisations(self, response):
        autorisations = []
        try:
            autorisations_section = response.xpath('//div[contains(., "Autorisations")]/following-sibling::div[1]')
            for row in autorisations_section.xpath('.//tr'):
                try:
                    autorisation = {
                        'denomination': row.xpath('./td[1]//text()').get(),
                        'date_debut': row.xpath('./td[2]//text()').get()
                    }
                    if autorisation['denomination']:
                        autorisations.append(autorisation)
                except Exception as e:
                    debug_print(f"Erreur sur une autorisation: {e}", "debug")
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des autorisations: {e}", "debug")
        return autorisations
    
    def extract_nace(self, response, version):
        codes = []
        try:
            nace_section = response.xpath(f'//div[contains(., "Code NACE {version}")]/following-sibling::div[1]')
            if not nace_section:
                nace_section = response.xpath(f'//div[contains(., "Codes NACE-BEL {version}")]/following-sibling::div[1]')
            
            for row in nace_section.xpath('.//tr'):
                try:
                    code = {
                        'code': row.xpath('./td[1]//text()').get(),
                        'description': row.xpath('./td[2]//text()').get(),
                        'date_debut': row.xpath('./td[3]//text()').get()
                    }
                    if code['code'] or code['description']:
                        codes.append(code)
                except Exception as e:
                    debug_print(f"Erreur sur un code NACE {version}: {e}", "debug")
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des codes NACE {version}: {e}", "debug")
        return codes
    
    def extract_donnees_financieres(self, response):
        donnees = {}
        try:
            financieres_section = response.xpath('//div[contains(., "Données financières")]/following-sibling::div[1]')
            rows = financieres_section.xpath('.//tr')
            for row in rows:
                try:
                    cle = row.xpath('./th//text()').get()
                    valeur = row.xpath('./td//text()').get()
                    if cle and valeur:
                        donnees[cle.strip()] = valeur.strip()
                except Exception as e:
                    debug_print(f"Erreur sur une donnée financière: {e}", "debug")
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des données financières: {e}", "debug")
        return donnees
    
    def extract_liens_entites(self, response):
        liens = []
        try:
            liens_section = response.xpath('//div[contains(., "Liens entre entités")]/following-sibling::div[1]')
            for row in liens_section.xpath('.//tr'):
                try:
                    lien = {
                        'numero_entreprise': row.xpath('./td[1]//text()').get(),
                        'denomination': row.xpath('./td[2]//text()').get(),
                        'type_lien': row.xpath('./td[3]//text()').get(),
                        'date_debut': row.xpath('./td[4]//text()').get()
                    }
                    if lien['numero_entreprise'] or lien['denomination']:
                        liens.append(lien)
                except Exception as e:
                    debug_print(f"Erreur sur un lien entre entités: {e}", "debug")
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des liens entre entités: {e}", "debug")
        return liens
    
    def extract_liens_externes(self, response):
        liens = []
        try:
            liens_section = response.xpath('//div[contains(., "Liens externes")]/following-sibling::div[1]')
            for row in liens_section.xpath('.//a'):
                try:
                    url = row.xpath('./@href').get()
                    texte = row.xpath('./text()').get()
                    if url:
                        liens.append({
                            'url': url,
                            'texte': texte
                        })
                except Exception as e:
                    debug_print(f"Erreur sur un lien externe: {e}", "debug")
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des liens externes: {e}", "debug")
        return liens
    
    def extract_fonctions(self, response):
        fonctions = []
        try:
            fonctions_section = response.xpath('//div[contains(@id, "functions") or contains(@class, "functions")]')
            for fonction in fonctions_section.xpath('.//tr[position()>1]'):
                try:
                    personne = {
                        'nom': fonction.xpath('./td[1]//text()').get(),
                        'role': fonction.xpath('./td[2]//text()').get(),
                        'date_debut': fonction.xpath('./td[3]//text()').get(),
                    }
                    fonctions.append(personne)
                except Exception as e:
                    debug_print(f"Erreur sur une fonction: {e}", "debug")
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des fonctions: {e}", "debug")
        return fonctions

    def extract_generalites(self, response):
        generalites = {}
        try:
            # Utiliser des sélecteurs plus larges pour capturer le contenu dans différentes structures HTML
            
            # Titre de l'entreprise (peut être dans plusieurs types d'en-têtes)
            generalites['nom'] = response.css('h1::text, h2::text').get() or response.xpath('//title/text()').get()
            if generalites['nom'] and ':' in generalites['nom']:
                generalites['nom'] = generalites['nom'].split(':')[1].strip()
            
            # Statut (actif/fermé)
            generalites['statut'] = response.css('.status::text, span.status::text').get() or \
                                response.xpath('//*[contains(@class, "status")]/text()').get() or \
                                response.xpath('//div[contains(., "Statut:")]/following-sibling::div/text()').get()
            
            # Forme juridique
            generalites['forme_juridique'] = response.xpath('//th[contains(., "Forme juridique")]/following-sibling::td/text()').get() or \
                                            response.xpath('//div[contains(., "Forme juridique:")]/following-sibling::div/text()').get()
                                            
            # Date de début
            generalites['date_debut'] = response.xpath('//th[contains(., "Date de début")]/following-sibling::td/text()').get() or \
                                    response.xpath('//div[contains(., "Date de début:")]/following-sibling::div/text()').get()
            
            # Essayer de trouver l'adresse
            adresse = []
            adresse_elements = response.css('.address::text, .addr::text').getall() or \
                            response.xpath('//*[contains(@class, "address")]/text() | //*[contains(@class, "addr")]/text()').getall() or \
                            response.xpath('//div[contains(., "Adresse:")]/following-sibling::div//text()').getall()
            
            for elem in adresse_elements:
                if elem and elem.strip():
                    adresse.append(elem.strip())
            
            generalites['adresse'] = ' '.join(adresse)
            
            # Si l'adresse est toujours vide, essayer un sélecteur plus générique
            if not generalites['adresse']:
                address_section = response.xpath('//div[contains(., "Adresse") or contains(., "adresse")]/following-sibling::div')
                if address_section:
                    address_text = ' '.join(address_section.xpath('.//text()').getall())
                    generalites['adresse'] = address_text.strip()
            
            debug_print(f"Nom extrait: {generalites.get('nom')}", "debug")
            debug_print(f"Statut extrait: {generalites.get('statut')}", "debug")
            debug_print(f"Adresse extraite: {generalites.get('adresse')}", "debug")
            debug_print(f"Forme juridique extraite: {generalites.get('forme_juridique')}", "debug")
            debug_print(f"Date de début extraite: {generalites.get('date_debut')}", "debug")
            
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des généralités: {e}", "error")
        
        # Validation: au moins un champ doit être non vide
        if not any(generalites.values()):
            debug_print("Aucune donnée extraite", "warning")
            return {}
            
        return generalites

# Spider 2: eJustice Spider 
class EjusticeSpider(scrapy.Spider):
    name = 'ejustice'
    allowed_domains = ['ejustice.just.fgov.be']
    
    # Désactiver la journalisation par défaut de Scrapy et ignorer robots.txt
    custom_settings = {
        'LOG_ENABLED': False,
        'ROBOTSTXT_OBEY': False  # Désactive l'obéissance au robots.txt
    }
    
    # Implémentez ce spider selon vos besoins spécifiques

# Spider 3: Consult Spider (NBB)
class ConsultSpider(scrapy.Spider):
    name = 'consult'
    allowed_domains = ['consult.cbso.nbb.be']
    
    custom_settings = {
        'LOG_ENABLED': False,
        'ROBOTSTXT_OBEY': True
    }
    
    # Implémentez ce spider selon vos besoins spécifiques