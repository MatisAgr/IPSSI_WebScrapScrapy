import scrapy

class EntrepriseItem(scrapy.Item):
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