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
            
            url = f'https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html?ondernemingsnummer={numero_clean}&lang=fr'
            
            if i % 10 == 0:  # Afficher seulement tous les 10 pour alléger
                debug_print(f"Requête KBO [{i+1}/{len(self.numeros_entreprise)}] pour {numero_clean}", "fetch")
            
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                headers={
                    'Accept-Language': 'fr-FR,fr;q=0.9',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                },
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
        
        # Analyser la structure de la page si c'est l'une des premières requêtes
        if scraping_stats.requests_success <= 2:
            self.analyze_page_structure(response)
        
        # Créer un nouvel item pour cette entreprise
        item = EntrepriseItem()
        item['numero_entreprise'] = numero_entreprise
        
        # Extraire les données par section avec gestion d'erreurs
        try:
            generalites = self.extract_generalites(response)
            if generalites:
                item['generalites'] = generalites
                debug_print(f"Généralités extraites avec succès", "success")
            else:
                item['generalites'] = {}
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des généralités: {str(e)}", "error")
            item['generalites'] = {}
        
        try:
            fonctions = self.extract_fonctions(response)
            if fonctions:
                item['fonctions'] = fonctions
                debug_print(f"{len(fonctions)} fonctions extraites", "success")
            else:
                item['fonctions'] = []
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des fonctions: {str(e)}", "error")
            item['fonctions'] = []
        
        try:
            qualites = self.extract_qualites(response)
            if qualites:
                item['qualites'] = qualites
                debug_print(f"{len(qualites)} qualités extraites", "success")
            else:
                item['qualites'] = []
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des qualités: {str(e)}", "error")
            item['qualites'] = []
        
        try:
            capacites = self.extract_capacites_entrepreneuriales(response)
            if capacites:
                item['capacites_entrepreneuriales'] = capacites
                debug_print(f"{len(capacites)} capacités entrepreneuriales extraites", "success")
            else:
                item['capacites_entrepreneuriales'] = []
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des capacités: {str(e)}", "error")
            item['capacites_entrepreneuriales'] = []
        
        try:
            autorisations = self.extract_autorisations(response)
            if autorisations:
                item['autorisations'] = autorisations
                debug_print(f"{len(autorisations)} autorisations extraites", "success")
            else:
                item['autorisations'] = []
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des autorisations: {str(e)}", "error")
            item['autorisations'] = []
        
        # NACE codes
        try:
            nace_2025 = self.extract_nace_2025(response)
            if nace_2025:
                item['nace_2025'] = nace_2025
                debug_print(f"{len(nace_2025)} codes NACE 2025 extraits", "success")
            else:
                item['nace_2025'] = []
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des codes NACE 2025: {str(e)}", "error")
            item['nace_2025'] = []
        
        try:
            nace_2008 = self.extract_nace_2008(response)
            if nace_2008:
                item['nace_2008'] = nace_2008
                debug_print(f"{len(nace_2008)} codes NACE 2008 extraits", "success")
            else:
                item['nace_2008'] = []
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des codes NACE 2008: {str(e)}", "error")
            item['nace_2008'] = []
        
        try:
            nace_2003 = self.extract_nace_2003(response)
            if nace_2003:
                item['nace_2003'] = nace_2003
                debug_print(f"{len(nace_2003)} codes NACE 2003 extraits", "success")
            else:
                item['nace_2003'] = []
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des codes NACE 2003: {str(e)}", "error")
            item['nace_2003'] = []
        
        try:
            donnees_financieres = self.extract_donnees_financieres(response)
            if donnees_financieres:
                item['donnees_financieres'] = donnees_financieres
                debug_print(f"Données financières extraites avec succès", "success")
            else:
                item['donnees_financieres'] = {}
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des données financières: {str(e)}", "error")
            item['donnees_financieres'] = {}
        
        try:
            liens_entites = self.extract_liens_entites(response)
            if liens_entites:
                item['liens_entites'] = liens_entites
                debug_print(f"{len(liens_entites)} liens entre entités extraits", "success")
            else:
                item['liens_entites'] = []
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des liens entre entités: {str(e)}", "error")
            item['liens_entites'] = []
        
        try:
            liens_externes = self.extract_liens_externes(response)
            if liens_externes:
                item['liens_externes'] = liens_externes
                debug_print(f"{len(liens_externes)} liens externes extraits", "success")
            else:
                item['liens_externes'] = []
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des liens externes: {str(e)}", "error")
            item['liens_externes'] = []
        
        # Champs pour les autres spider (à remplir ultérieurement)
        item['publications'] = []
        item['comptes_annuels'] = []
        
        # Si des données ont été extraites, yielder l'item
        if generalites or any(isinstance(value, list) and value for value in item.values()):
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
    
    # --- Extraire les données spécifiques de la page ---

    def extract_capacites_entrepreneuriales(self, response):
        capacites = []
        try:
            # Trouver la section des capacités entrepreneuriales
            section_header = response.xpath('//div[@id="table"]/table/tbody/tr/td[@class="I" and h2[contains(text(), "Capacités entrepreneuriales")]]/ancestor::tr')
            
            if not section_header:
                return []
            
            # Examiner la ligne suivante pour voir si elle contient des données
            next_row = section_header.xpath('following-sibling::tr[1]')
            if next_row:
                text_content = ''.join(next_row.xpath('.//text()').getall()).strip()
                
                # Si "Pas de données", retourner liste vide
                if "Pas de données reprises dans la BCE" in text_content:
                    return []
                
                # Sinon, extraire les capacités
                capacite = {
                    'denomination': text_content,
                    'categorie': None,
                    'date_debut': None
                }
                
                # Chercher une date éventuelle
                depuis = next_row.xpath('.//span[@class="upd"]/text()').get()
                if depuis and "Depuis le" in depuis:
                    capacite['date_debut'] = depuis.replace('Depuis le ', '').strip()
                
                capacites.append(capacite)
            
            debug_print(f"Nombre de capacités entrepreneuriales extraites: {len(capacites)}", "debug")
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des capacités entrepreneuriales: {e}", "error")
        
        return capacites
    
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
    
    def extract_donnees_financieres(self, response):
        donnees = {}
        try:
            # Trouver la section des données financières
            section_header = response.xpath('//div[@id="table"]/table/tbody/tr/td[@class="I" and h2[text()="Données financières"]]/ancestor::tr')
            
            if not section_header:
                return {}
            
            # Traiter les lignes suivantes jusqu'à la prochaine section
            next_rows = section_header.xpath('following-sibling::tr')
            
            for row in next_rows:
                # S'arrêter si on trouve une nouvelle section ou une ligne vide
                if row.xpath('./td[@class="I" and h2]') or row.xpath('./td[contains(text(), "&nbsp;")]'):
                    break
                
                # Extraire la clé (première cellule)
                key_cell = row.xpath('./td[1]/text()').get()
                if not key_cell:
                    continue
                
                key = key_cell.strip()
                
                # Extraire la valeur (cellules suivantes)
                value = ''.join(row.xpath('./td[position()>1]//text()').getall()).strip()
                
                if key and value:
                    donnees[key] = value
            
            debug_print(f"Données financières extraites: {donnees}", "debug")
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des données financières: {e}", "error")
        
        return donnees

    def extract_liens_entites(self, response):
        liens = []
        try:
            # Trouver la section des liens entre entités
            section_header = response.xpath('//div[@id="table"]/table/tbody/tr/td[@class="I" and h2[text()="Liens entre entités"]]/ancestor::tr')
            
            if not section_header:
                return []
            
            # Examiner la ligne suivante pour voir si elle contient des données
            next_row = section_header.xpath('following-sibling::tr[1]')
            if next_row:
                text_content = ''.join(next_row.xpath('.//text()').getall()).strip()
                
                # Si "Pas de données", retourner liste vide
                if "Pas de données reprises dans la BCE" in text_content:
                    return []
                
                # Sinon, extraire les liens entre entités
                # Cette partie dépend de la structure exacte quand il y a des liens
                for row in next_row.xpath('.//table//tr'):
                    cells = row.xpath('./td')
                    if len(cells) >= 4:
                        lien = {
                            'numero_entreprise': cells[0].xpath('.//text()').get(),
                            'denomination': cells[1].xpath('.//text()').get(),
                            'type_lien': cells[2].xpath('.//text()').get(),
                            'date_debut': cells[3].xpath('.//text()').get()
                        }
                        
                        # Nettoyer les valeurs
                        for key in lien:
                            if lien[key]:
                                lien[key] = lien[key].strip()
                        
                        if lien['numero_entreprise'] or lien['denomination']:
                            liens.append(lien)
            
            debug_print(f"Nombre de liens entre entités extraits: {len(liens)}", "debug")
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des liens entre entités: {e}", "error")
        
        return liens
    
    def extract_liens_externes(self, response):
        liens = []
        try:
            # Simplification du sélecteur XPath
            section_header = response.xpath('//tr/td[@class="I" and h2[text()="Liens externes"]]/ancestor::tr')
            
            if not section_header:
                debug_print("Section 'Liens externes' non trouvée", "debug")
                return []
            
            # Traiter la ligne suivante qui contient les liens
            next_row = section_header.xpath('following-sibling::tr[1]')
            if next_row:
                # Extraire tous les liens avec la classe "external"
                for link in next_row.xpath('.//a[@class="external"]'):
                    url = link.xpath('@href').get()  # Simplification du sélecteur
                    text = link.xpath('text()').get() # Simplification du sélecteur
                    
                    if url and text:
                        liens.append({
                            'url': url.strip(),
                            'texte': text.strip()
                        })
            
            debug_print(f"Nombre de liens externes extraits: {len(liens)}", "debug")
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des liens externes: {e}", "error")
            debug_print(f"Détails de l'erreur: {repr(e)}", "error")  # Afficher plus de détails
        
        return liens

    def extract_generalites(self, response):
        generalites = {}
        try:
            # Approche plus robuste: chercher directement les paires clé-valeur dans la section des généralités
            section_generalites = response.xpath('//tr/td[@class="I" and h2[text()="Généralités"]]/ancestor::table//tr')
            
            # Mapper les clés aux noms de champs
            mapping = {
                "Numéro d'entreprise:": "numero_entreprise",
                "Statut:": "statut",
                "Situation juridique:": "situation_juridique",
                "Date de début:": "date_debut",
                "Dénomination:": "denomination",
                "Abréviation:": "abreviation",
                "Adresse du siège:": "adresse",
                "Type d'entité:": "type_entite",
                "Forme légale:": "forme_legale",
                "Nombre d'unités d'établissement": "nombre_ue"
            }
            
            for row in section_generalites:
                # Chercher la cellule avec une clé
                key_cell = row.xpath('./td[contains(@class, "QL") or contains(@class, "RL")]/text()').get()
                
                if not key_cell:
                    continue
                    
                key_cell = key_cell.strip()
                
                # Si cette clé fait partie de notre mapping
                for original_key, mapped_field in mapping.items():
                    if original_key in key_cell:
                        # Extraire la valeur (différentes stratégies selon le champ)
                        if mapped_field == "statut" or mapped_field == "situation_juridique":
                            value = row.xpath('./td[position()>1]//span[@class="pageactief"]/text()').get()
                        elif mapped_field == "nombre_ue":
                            value = row.xpath('./td[position()>1]/strong/text()').get()
                        else:
                            # Valeur de texte standard
                            value = row.xpath('./td[position()>1]/text()[1]').get()
                            
                        # Nettoyer la valeur
                        if value:
                            generalites[mapped_field] = value.strip()
                        
                        # Extraire les informations supplémentaires (dates "depuis")
                        if mapped_field == "situation_juridique":
                            depuis = row.xpath('./td[position()>1]//span[@class="upd"]/text()').get()
                            if depuis and "Depuis le" in depuis:
                                generalites["situation_juridique_depuis"] = depuis.replace('Depuis le ', '').strip()
                        
                        # Pour l'adresse, combiner les lignes
                        if mapped_field == "adresse":
                            all_text = row.xpath('./td[position()>1]//text()[not(parent::span[@class="upd"])]').getall()
                            if all_text:
                                generalites["adresse"] = ' '.join([t.strip() for t in all_text if t.strip()])
                        break
            
            debug_print(f"Généralités extraites: {generalites}", "debug")
        
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des généralités: {e}", "error")
        
        return generalites
    
    def extract_fonctions(self, response):
        fonctions = []
        try:
            # D'abord, chercher les informations sur le nombre de fonctions (même si le tableau est caché)
            fonctions_info = response.xpath('//span[@id="klikfctie"]/text()').get()
            if fonctions_info:
                debug_print(f"Information sur les fonctions: {fonctions_info}", "info")
            
            # Essayer plusieurs approches pour trouver le tableau des fonctions
            # 1. Chercher le tableau directement visible
            table_fonctions = response.xpath('//table[@id="toonfctie"]')
            
            # 2. Chercher le tableau même s'il est caché
            if not table_fonctions:
                table_fonctions = response.xpath('//table[contains(@id, "toonfctie")]')
                
            # 3. Si toujours pas trouvé, essayer en se basant sur la structure parent
            if not table_fonctions:
                table_fonctions = response.xpath('//tr[td/span[@id="klikfctie"]]/following-sibling::tr[1]//table')
            
            if table_fonctions:
                # Extraire les données des lignes du tableau
                for row in table_fonctions.xpath('.//tr'):
                    cells = row.xpath('./td')
                    if len(cells) >= 3:
                        fonction = {
                            'role': cells[0].xpath('.//text()').get(),
                            'nom': cells[1].xpath('.//text()').get(),
                            'depuis': None
                        }
                        
                        # Extraire la date (dans un span spécifique)
                        depuis = cells[2].xpath('.//span[@class="upd"]/text()').get()
                        if depuis:
                            fonction['depuis'] = depuis.replace('Depuis le ', '').strip()
                            
                        # Nettoyer les valeurs
                        for key in fonction:
                            if fonction[key]:
                                fonction[key] = ' '.join(fonction[key].strip().split())
                        
                        # Ajouter seulement si nous avons au moins un rôle ou un nom
                        if fonction['role'] or fonction['nom']:
                            fonctions.append(fonction)
            
            debug_print(f"Nombre de fonctions extraites: {len(fonctions)}", "debug")
        
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des fonctions: {e}", "error")
        
        return fonctions
    
    def extract_qualites(self, response):
        qualites = []
        try:
            # Correction du sélecteur - rechercher au niveau tr sans div
            section_header = response.xpath('//tr/td[@class="I" and h2[text()="Qualités"]]/ancestor::tr')
            
            if not section_header:
                debug_print("Section 'Qualités' non trouvée", "debug")
                return []
            
            # Traiter les lignes suivantes jusqu'à la prochaine section
            next_rows = section_header.xpath('following-sibling::tr')
            
            for row in next_rows:
                # S'arrêter si on trouve une nouvelle section
                if row.xpath('./td[@class="I" and h2]'):
                    break
                    
                # Ignorer les lignes vides ou "Pas de données"
                text_content = ''.join(row.xpath('.//text()').getall()).strip()
                if not text_content or "Pas de données reprises dans la BCE" in text_content:
                    continue
                    
                # Extraire la qualité (simplification du sélecteur)
                qualite_text = row.xpath('.//text()[not(ancestor::span[@class="upd"])]').getall()
                if qualite_text:
                    qualite_text = ''.join(qualite_text).strip()
                    
                    # Extraire la date "depuis"
                    depuis = row.xpath('.//span[@class="upd"]/text()').get()
                    depuis_value = None
                    if depuis and "Depuis le" in depuis:
                        depuis_value = depuis.replace('Depuis le ', '').strip()
                        
                    qualites.append({
                        'qualite': qualite_text,
                        'depuis': depuis_value
                    })
            
            debug_print(f"Nombre de qualités extraites: {len(qualites)}", "debug")
        
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des qualités: {e}", "error")
        
        return qualites
    
    def extract_nace_2025(self, response):
        codes = []
        try:
            # Simplification des sélecteurs XPath
            sections = [
                # Pour les activités TVA
                '//tr/td[@class="I" and contains(h2/text(), "Activités TVA Code Nacebel version 2025")]',
                # Pour les activités ONSS
                '//tr/td[@class="I" and contains(h2/text(), "Activités ONSS Code Nacebel version 2025")]'
            ]
            
            for section_xpath in sections:
                section = response.xpath(section_xpath)
                if not section:
                    continue
                    
                section_tr = section.xpath('./ancestor::tr')
                if not section_tr:
                    continue
                    
                # Récupérer la ligne suivante contenant les données
                code_row = section_tr.xpath('following-sibling::tr[1]')
                if not code_row:
                    continue
                    
                # Extraire le texte complet pour analyse
                row_text = ''.join(code_row.xpath('.//text()').getall()).strip()
                if "Pas de données" in row_text:
                    continue
                    
                # Déterminer le type (TVA ou ONSS)
                nace_type = "TVA" if "TVA" in row_text else "ONSS"
                
                # Extraire le code NACE (dans le lien ou directement du texte)
                code = code_row.xpath('.//a[contains(@href, "nace.code=")]/text()').get()
                if not code:
                    # Extraction du code à partir du texte formaté comme "TYPE2025 CODE - DESCRIPTION"
                    import re
                    code_match = re.search(r'(TVA|ONSS)\D*(\d+\.\d+)', row_text)
                    if code_match:
                        code = code_match.group(2)
                
                # Extraire la description (après le tiret)
                description = None
                if '-' in row_text:
                    parts = row_text.split('-')
                    if len(parts) > 1:
                        description = parts[1].strip()
                
                # Extraire la date depuis
                depuis = code_row.xpath('.//span[@class="upd"]/text()').get()
                depuis_value = None
                if depuis and "Depuis le" in depuis:
                    depuis_value = depuis.replace('Depuis le ', '').strip()
                
                if code and description:
                    codes.append({
                        'type': nace_type,
                        'code': code.strip(),
                        'description': description,
                        'depuis': depuis_value
                    })
            
            debug_print(f"Nombre de codes NACE 2025 extraits: {len(codes)}", "debug")
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des codes NACE 2025: {e}", "error")
        
        return codes

    def extract_nace_2008(self, response):
        codes = []
        try:
            # La table des codes NACE 2008 a un ID spécifique
            nace_table = response.xpath('//table[@id="toonbtw2008"]')
            
            if nace_table:
                # Chercher les sections TVA et ONSS
                sections = [
                    './/tr/td[@class="I" and h2[contains(text(), "Activités TVA Code Nacebel version 2008")]]/ancestor::tr',
                    './/tr/td[@class="I" and h2[contains(text(), "Activités ONSS Code Nacebel version 2008")]]/ancestor::tr'
                ]
                
                for section_xpath in sections:
                    section = nace_table.xpath(section_xpath)
                    if section:
                        # Traiter la ligne suivante qui contient le code
                        code_row = section.xpath('following-sibling::tr[1]')
                        if code_row:
                            # Extraire toutes les données textuelles de la ligne
                            row_text = ''.join(code_row.xpath('.//text()').getall()).strip()
                            
                            # Extraire le code NACE
                            code_parts = row_text.split('-')
                            code_text = None
                            if len(code_parts) > 0:
                                code_text_parts = code_parts[0].split()
                                if len(code_text_parts) > 1:
                                    code_text = code_text_parts[-1].strip()
                            
                            # Extraire la description (après le tiret)
                            description = None
                            if '-' in row_text:
                                parts = row_text.split('-')
                                if len(parts) > 1:
                                    description = parts[-1].strip()
                            
                            # Extraire la date
                            depuis = code_row.xpath('.//span[@class="upd"]/text()').get()
                            depuis_value = None
                            if depuis and "Depuis le" in depuis:
                                depuis_value = depuis.replace('Depuis le ', '').strip()
                            
                            if code_text and description:
                                # Déterminer le type (TVA ou ONSS)
                                nace_type = "TVA" if "TVA" in row_text else "ONSS" 
                                
                                codes.append({
                                    'type': nace_type,
                                    'code': code_text,
                                    'description': description,
                                    'depuis': depuis_value
                                })
            
            debug_print(f"Nombre de codes NACE 2008 extraits: {len(codes)}", "debug")
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des codes NACE 2008: {e}", "error")
        
        return codes

    def extract_nace_2003(self, response):
        codes = []
        try:
            # La table des codes NACE 2003 a un ID spécifique
            nace_table = response.xpath('//table[@id="toonbtw"]')
            
            if nace_table:
                # Chercher la section TVA
                section = nace_table.xpath('.//tr/td[@class="I" and h2[contains(text(), "Activités TVA Code Nacebel version 2003")]]/ancestor::tr')
                if section:
                    # Traiter la ligne suivante qui contient le code
                    code_row = section.xpath('following-sibling::tr[1]')
                    if code_row:
                        # Extraire toutes les données textuelles de la ligne
                        row_text = ''.join(code_row.xpath('.//text()').getall()).strip()
                        
                        # Extraire le code NACE
                        code_parts = row_text.split('-')
                        code_text = None
                        if len(code_parts) > 0:
                            code_text_parts = code_parts[0].split()
                            if len(code_text_parts) > 1:
                                code_text = code_text_parts[-1].strip()
                        
                        # Extraire la description (après le tiret)
                        description = None
                        if '-' in row_text:
                            parts = row_text.split('-')
                            if len(parts) > 1:
                                description = parts[-1].strip()
                        
                        # Extraire la date
                        depuis = code_row.xpath('.//span[@class="upd"]/text()').get()
                        depuis_value = None
                        if depuis and "Depuis le" in depuis:
                            depuis_value = depuis.replace('Depuis le ', '').strip()
                        
                        if code_text and description:
                            codes.append({
                                'type': "TVA",
                                'code': code_text,
                                'description': description,
                                'depuis': depuis_value
                            })
            
            debug_print(f"Nombre de codes NACE 2003 extraits: {len(codes)}", "debug")
        except Exception as e:
            debug_print(f"Erreur lors de l'extraction des codes NACE 2003: {e}", "error")
        
        return codes

        def extract_nace_hidden(self, response, version):
            """Extraction des codes NACE qui peuvent être cachés dans des sections repliées"""
            codes = []
            try:
                # Pour les codes NACE 2008 et 2003 qui sont souvent cachés
                if version in ["2008", "2003"]:
                    table_id = f"toonbtw{version}"
                    nace_table = response.xpath(f'//table[@id="{table_id}"]')
                    
                    if nace_table:
                        for row in nace_table.xpath('.//tr'):
                            code_text = row.xpath('./td[1]/text()').get()
                            description = row.xpath('./td[2]/text()').get()
                            depuis = row.xpath('.//span[@class="upd"]/text()').get()
                            
                            if code_text and description:
                                code = {
                                    'code': code_text.strip(),
                                    'description': description.strip(),
                                    'depuis': depuis.replace('Depuis le ', '').strip() if depuis else None
                                }
                                codes.append(code)
                
                debug_print(f"Nombre de codes NACE {version} cachés extraits: {len(codes)}", "debug")
            
            except Exception as e:
                debug_print(f"Erreur lors de l'extraction des codes NACE {version} cachés: {e}", "error")
            
            return codes

        def extract_hidden_content(self, response, section_id):
            """Extrait le contenu caché qui nécessiterait normalement un clic JavaScript"""
            content = {}
            try:
                # Les contenus cachés ont généralement un id spécifique et display:none
                hidden_element = response.xpath(f'//*[@id="{section_id}" and contains(@style, "display: none")]')
                
                if not hidden_element:
                    # Essayer de trouver l'élément sans vérifier le style (il peut être dans le HTML mais caché)
                    hidden_element = response.xpath(f'//*[@id="{section_id}"]')
                
                if hidden_element:
                    # Récupérer tous le contenu texte de cet élément
                    content['html'] = hidden_element.get()
                    content['text'] = ''.join(hidden_element.xpath('.//text()').getall())
                    
                    # Rechercher des tables
                    tables = hidden_element.xpath('.//table')
                    if tables:
                        content['tables_count'] = len(tables)
                        
                    # Rechercher des liens 
                    links = hidden_element.xpath('.//a/@href').getall()
                    if links:
                        content['links'] = links
                        
                    debug_print(f"Contenu caché trouvé pour {section_id}: {len(content['text'])} caractères", "debug")
                else:
                    debug_print(f"Aucun contenu caché trouvé pour {section_id}", "debug")
            
            except Exception as e:
                debug_print(f"Erreur lors de l'extraction du contenu caché {section_id}: {e}", "error")
            
            return content

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