import csv
import config
from redis import Redis

filepath = 'data/'

productsfile = filepath + 'products.csv'
catsfile = filepath + 'categories.csv'
imagesfile = filepath + 'images.csv'


count = 0

def addCounter():
     global count
     count = count + 1
     return count

def main():

    # Setup Redis connection
    host = config.REDIS_CFG["host"]
    port = config.REDIS_CFG["port"]
    pwd = config.REDIS_CFG["password"]
    redis = Redis(host=host, port=port, password=pwd, charset="utf-8", decode_responses=True)

    # Flush everything from the database
    print("Flushing the database...")
    redis.flushall()

    print ("Importing products...")
    import_products(redis)

    print("Importing categories...")
    import_categories(redis)

    print("Importing images...")
    import_images(redis)

    print("Import of {} records completed.".format(count))

    searchProductId = '6'
    print("Finding products by ID: " + searchProductId)
    find_product_by_id(redis, searchProductId)

    #searchCategory = 'Toys'
    #searchCategory = 'Food & beverages'
    searchCategory = 'Computers & Electronics'
    print("Finding products by category: " + searchCategory)
    find_products_by_category(redis, searchCategory)

    searchName= 'widescreen'
    print("Finding products by name: " + searchName)
    find_products_by_name(redis, searchName)


# function to import the products datasource
# arguments: redis client
def import_products(redis):

    with open(productsfile, 'r') as products:
        reader = csv.DictReader(products, delimiter=";")

        for row in reader:
            _id = row["id"]
            name = row["name"]
            description = row["description"]
            vendor = row["vendor"]
            price = row["price"]
            currency = row["currency"]
            category = row["category"]
            images = row["images"]

            redis.hset('product:' + _id,
              mapping={
                  '_id': str(_id),
                  '_type': 'product',
                  'name': name,
                  'description': description,
                  'vendor': vendor,
                  'price': price,
                  'currency': currency,
                  '^category': category,
                  '^images': images
              })

            #create a product name index
            redis.hset('idx_product_name',
               mapping={
                   name: str(_id),
               })

            #create a products per category index
            redis.sadd('idx_category:' + category, 'product:' + _id)

            addCounter()

# function to import the categories datasource
# arguments: redis client
def import_categories(redis):

    with open(catsfile, 'r') as categories:
        reader = csv.DictReader(categories, delimiter=";")

        for row in reader:
            _id = row["id"]
            name = row["name"]

            redis.hset('category:' + _id,
              mapping={
                  '_id': str(_id),
                  '_type': 'category',
                  'name': name,
              })

            addCounter()

# function to import the images datasource
# arguments: redis client
def import_images(redis):

    with open(imagesfile, 'r') as images:
        reader = csv.DictReader(images, delimiter=";")

        for row in reader:
            _id = row["id"]
            value = row["value"]

            redis.hset('image:' + _id,
              mapping={
                  '_id': str(_id),
                  '_type': 'image',
                  'value': value,
              })

            addCounter()

def find_product_by_id(redis, _id):
    print(redis.hgetall('product:' + _id))


def find_products_by_category(redis, category):
    products = redis.smembers('idx_category:' + category)

    for product in products:
        print(redis.hgetall(product))


def find_products_by_name(redis, name):
    for product in redis.hscan_iter('idx_product_name', match='*' + name + '*', count=1000):
        productsList = list(product)
        productId = productsList[1]
        print(redis.hgetall('product:' + productId))


if __name__=="__main__":
    main()


