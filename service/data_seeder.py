import os, gzip, json, time
from service.neo4jconnector import neo4j_con
from settings import project_root
from copy import deepcopy
import pandas as pd

def new_prod_meta():
    """
    :return: Empty meta data structure as dict.
    """
    return {
        "asin": "NA",
        "group": "NA",
        "salesrank": "NA",
        "product_id": "NA",
        "title": "NA",
        "reviews": {},
        "similar": {},
        "categories": {}
    }

def test_pandas(meta_file_name='amazon-meta.txt.gz'):
    meta_file_path = os.path.join(project_root, 'public', meta_file_name)
    chunksize = 10 ** 6
    reader = pd.read_table(meta_file_path, compression='gzip', sep='\n', index_col=False, header=None, engine='python', chunksize=chunksize)

    for chunk in reader:
        for line in chunk.values:
            print line[0]
        break

def parse_create_schema_from_meta_file_using_gzip(meta_file_name='amazon-meta.txt.gz',
                                 similarity_mappings_file_name='similarity_mappings.txt'):
    """
    Open `meta file` containing products' information, parse it and structures the information in the form of dictionary.
    In parallel, crete a `similarity_mappings file` which contains information about similar products and later can be converted
    into neo4j relationship named `SIMILAR_TO`.
    :param meta_file_path: Absolute path of meta file
    :param similarity_mappings_file_path:   Absolute path where similarity_mappings will be stored.
    :return: Python generator containing meta data as dictionary.
    """
    q = """
        MERGE (prod1:`Product`{
        categories_count: {categories_count}, 
        asin: {asin}, group: {group}, product_id: {product_id}, title: {title}, avg_rating: {avg_rating}, 
        salesrank: {salesrank}, reviews_total: {reviews_total}, reviews_downloaded: {reviews_downloaded}
        })
        MERGE (cat1:`Category`{id: {cat}.id, name: {cat}.name})
        MERGE (prod1) -[:BELONGS_TO]-> (cat1)
        WITH {customers_review_details} as crd_with, prod1
        UNWIND crd_with AS crd
        MERGE (cust1:`Customer`{id: crd.customer_id})
        MERGE (prod1) -[:`REVIEWED_BY`{rating: crd.rating, votes: crd.votes, helpful: crd.helpful, date: crd.date}]-> (cust1)
        """

    count = 0
    params_list = []

    meta_file_path = os.path.join(project_root, 'public', meta_file_name)
    similarity_mappings_file_path = os.path.join(project_root, 'public', similarity_mappings_file_name)

    chunksize = 10 ** 6
    with gzip.open(meta_file_path) as infile, open(similarity_mappings_file_path, 'w') as outfile:
        prod_meta = new_prod_meta()
        for line in infile:
            line = line.strip()
            #print line
            if line.startswith("Id:"):
                if len(prod_meta) > 0:
                    cat_details = prod_meta["categories"].get("details", {})
                    cat_id = cat_name = 'NA'
                    if len(cat_details.get("top", [])):
                        cat_id = cat_details['top'][0][0]
                        cat_name = cat_details['top'][0][1]
                    cat = {
                        "id": cat_id,
                        "name": cat_name
                    }
                    customers_review_details = prod_meta['reviews'].get("customers_review_details", [])
                    params = {
                        "product_id": prod_meta.get('product_id', 'NA'),
                        "asin": prod_meta.get('asin', 'NA'),
                        "group": prod_meta.get('group', 'NA'),
                        "salesrank": prod_meta.get('salesrank', 'NA'),
                        "title": prod_meta.get('title', 'NA'),
                        "reviews_total": prod_meta['reviews'].get('total', 'NA'),
                        "reviews_downloaded": prod_meta['reviews'].get('downloaded', 'NA'),
                        "avg_rating": prod_meta['reviews'].get('avg_rating', 'NA'),
                        "categories_count": prod_meta["categories"].get("count", 'NA'),
                        "cat": cat,
                        "customers_review_details": customers_review_details
                    }
                    params_list.append(params)
                    prod_meta = new_prod_meta()
                    count += 1
                    if count == 500:
                        execute_batch(q, deepcopy(params_list))
                        params_list = []
                        count = 0
                prod_meta['product_id'] = line.split()[1]
            elif line.startswith("ASIN:"):
                prod_meta['asin'] = line.split()[1]
            elif line.startswith("title:"):
                prod_meta['title'] = line.split("title:")[1].strip()
            elif line.startswith("group:"):
                prod_meta['group'] = line.split()[1]
            elif line.startswith("salesrank:"):
                prod_meta['salesrank'] = line.split()[1]
            elif line.startswith("similar:"):
                sim_line = line.split()
                sim_prods_asins = sim_line[2:]
                prod_meta['similar'] = {
                    'total': sim_line[1],
                    'asins': sim_prods_asins
                }
                if len(sim_prods_asins) > 0:
                    sim_dict = {prod_meta['product_id']: sim_prods_asins}
                    oline = json.dumps(sim_dict) + "\n"
                    # oline = "{product_id}:{similar_asins}\n".format(product_id=prod_meta['product_id'],
                    #                                                similar_asins=sim_prods_asins)
                    outfile.write(oline)
            elif line.startswith("categories:"):
                prod_meta['categories'] = prod_meta.get('categories', {})
                prod_meta['categories']['count'] = int(line.split()[1])
            elif line.startswith("|"):
                prod_meta['categories'] = prod_meta.get('categories', {})
                prod_meta['categories']['details'] = prod_meta['categories'].get("details", {"top": [], "sub": []})
                categories_name_ids = line.split("|")
                for cat_name_id in categories_name_ids:
                    cat_name = cat_name_id[:cat_name_id.find('[')]
                    cat_id = cat_name_id[cat_name_id.find('[') + 1:cat_name_id.find(']')]
                    if len(cat_name) > 0 and len(cat_id) > 0:
                        if len(prod_meta['categories']['details']["top"]) == 0:
                            prod_meta['categories']['details']['top'].append((cat_id, cat_name))
                        else:
                            prod_meta['categories']['details']["sub"].append((cat_id, cat_name))

            elif line.startswith("reviews:"):
                prod_meta['reviews'] = prod_meta.get('reviews', {})
                review_stats = line.split()
                prod_meta['reviews']['total'] = int(review_stats[2])
                prod_meta['reviews']['downloaded'] = int(review_stats[4])
                prod_meta['reviews']['avg_rating'] = float(review_stats[7])
            elif line.find("cutomer:") > -1:
                prod_meta['reviews'] = prod_meta.get('reviews', {})
                prod_meta['reviews']['customers_review_details'] = prod_meta['reviews'].get("customers_review_details",
                                                                                            [])
                review_info = line.split()
                prod_meta['reviews']['customers_review_details'].append({
                    "date": review_info[0],
                    "customer_id": review_info[2],  # str(self.customer_map[review_info[2]])
                    "rating": int(review_info[4]),
                    "votes": int(review_info[6]),
                    "helpful": int(review_info[8])
                })


def parse_create_schema_from_meta_file_using_pandas(meta_file_name='amazon-meta.txt.gz',
                                 similarity_mappings_file_name='similarity_mappings.txt'):
    """
    Open `meta file` containing products' information, parse it and structures the information in the form of dictionary.
    In parallel, crete a `similarity_mappings file` which contains information about similar products and later can be converted
    into neo4j relationship named `SIMILAR_TO`.
    :param meta_file_path: Absolute path of meta file
    :param similarity_mappings_file_path:   Absolute path where similarity_mappings will be stored.
    :return: Python generator containing meta data as dictionary.
    """
    q = """
        MERGE (prod1:`Product`{
        categories_count: {categories_count}, 
        asin: {asin}, group: {group}, product_id: {product_id}, title: {title}, avg_rating: {avg_rating}, 
        salesrank: {salesrank}, reviews_total: {reviews_total}, reviews_downloaded: {reviews_downloaded}
        })
        MERGE (cat1:`Category`{id: {cat}.id, name: {cat}.name})
        MERGE (prod1) -[:BELONGS_TO]-> (cat1)
        WITH {customers_review_details} as crd_with, prod1
        UNWIND crd_with AS crd
        MERGE (cust1:`Customer`{id: crd.customer_id})
        MERGE (prod1) -[:`REVIEWED_BY`{rating: crd.rating, votes: crd.votes, helpful: crd.helpful, date: crd.date}]-> (cust1)
        """

    count = 0
    params_list = []

    meta_file_path = os.path.join(project_root, 'public', meta_file_name)
    similarity_mappings_file_path = os.path.join(project_root, 'public', similarity_mappings_file_name)

    chunksize = 10 ** 6
    reader = pd.read_table(meta_file_path, compression='gzip', sep='\n', index_col=False, header=None, engine='python',
                           chunksize=chunksize)

    prod_meta = new_prod_meta()
    for chunk in reader:
        for lines in chunk.values:
            line = lines[0].strip()

            #print line
            if line.startswith("Id:"):
                if len(prod_meta) > 0:
                    cat_details = prod_meta["categories"].get("details", {})
                    cat_id = cat_name = 'NA'
                    if len(cat_details.get("top", [])):
                        cat_id = cat_details['top'][0][0]
                        cat_name = cat_details['top'][0][1]
                    cat = {
                        "id": cat_id,
                        "name": cat_name
                    }
                    customers_review_details = prod_meta['reviews'].get("customers_review_details", [])
                    params = {
                        "product_id": prod_meta.get('product_id', 'NA'),
                        "asin": prod_meta.get('asin', 'NA'),
                        "group": prod_meta.get('group', 'NA'),
                        "salesrank": prod_meta.get('salesrank', 'NA'),
                        "title": prod_meta.get('title', 'NA'),
                        "reviews_total": prod_meta['reviews'].get('total', 'NA'),
                        "reviews_downloaded": prod_meta['reviews'].get('downloaded', 'NA'),
                        "avg_rating": prod_meta['reviews'].get('avg_rating', 'NA'),
                        "categories_count": prod_meta["categories"].get("count", 'NA'),
                        "cat": cat,
                        "customers_review_details": customers_review_details
                    }
                    params_list.append(params)
                    prod_meta = new_prod_meta()
                    count += 1
                    if count == 500:
                        execute_batch(q, deepcopy(params_list))
                        params_list = []
                        count = 0
                prod_meta['product_id'] = line.split()[1]
            elif line.startswith("ASIN:"):
                prod_meta['asin'] = line.split()[1]
            elif line.startswith("title:"):
                prod_meta['title'] = line.split("title:")[1].strip()
            elif line.startswith("group:"):
                prod_meta['group'] = line.split()[1]
            elif line.startswith("salesrank:"):
                prod_meta['salesrank'] = line.split()[1]
            elif line.startswith("similar:"):
                sim_line = line.split()
                sim_prods_asins = sim_line[2:]
                prod_meta['similar'] = {
                    'total': sim_line[1],
                    'asins': sim_prods_asins
                }
                if len(sim_prods_asins) > 0:
                    sim_dict = {prod_meta['product_id']: sim_prods_asins}
                    oline = json.dumps(sim_dict) + "\n"
                    # oline = "{product_id}:{similar_asins}\n".format(product_id=prod_meta['product_id'],
                    #                                                similar_asins=sim_prods_asins)
                    # outfile.write(oline)
            elif line.startswith("categories:"):
                prod_meta['categories'] = prod_meta.get('categories', {})
                prod_meta['categories']['count'] = int(line.split()[1])
            elif line.startswith("|"):
                prod_meta['categories'] = prod_meta.get('categories', {})
                prod_meta['categories']['details'] = prod_meta['categories'].get("details", {"top": [], "sub": []})
                categories_name_ids = line.split("|")
                for cat_name_id in categories_name_ids:
                    cat_name = cat_name_id[:cat_name_id.find('[')]
                    cat_id = cat_name_id[cat_name_id.find('[') + 1:cat_name_id.find(']')]
                    if len(cat_name) > 0 and len(cat_id) > 0:
                        if len(prod_meta['categories']['details']["top"]) == 0:
                            prod_meta['categories']['details']['top'].append((cat_id, cat_name))
                        else:
                            prod_meta['categories']['details']["sub"].append((cat_id, cat_name))

            elif line.startswith("reviews:"):
                prod_meta['reviews'] = prod_meta.get('reviews', {})
                review_stats = line.split()
                prod_meta['reviews']['total'] = int(review_stats[2])
                prod_meta['reviews']['downloaded'] = int(review_stats[4])
                prod_meta['reviews']['avg_rating'] = float(review_stats[7])
            elif line.find("cutomer:") > -1:
                prod_meta['reviews'] = prod_meta.get('reviews', {})
                prod_meta['reviews']['customers_review_details'] = prod_meta['reviews'].get("customers_review_details",
                                                                                            [])
                review_info = line.split()
                prod_meta['reviews']['customers_review_details'].append({
                    "date": review_info[0],
                    "customer_id": review_info[2],  # str(self.customer_map[review_info[2]])
                    "rating": int(review_info[4]),
                    "votes": int(review_info[6]),
                    "helpful": int(review_info[8])
                })

def execute_batch(q, params_list):
    print "\nExecuting batch of size : {size} @ {time}".format(size=len(params_list), time=time.asctime())
    tx = neo4j_con.begin()
    for params in params_list:
        tx.append(q, params)
    tx.commit()
    print "Processes a batch of size : {size} @ {time}\n".format(size=len(params_list), time=time.asctime())


def parse_prod_similarity_mappings_file(similarity_mappings_file_name='similarity_mappings.txt'):
    """
    Parse `similarity_mappings` file and yield each entry.
    :param similarity_mappings_file_name: Name of `similarity_mappings` file
    :return: Generator
    """
    similarity_mappings_file_path = os.path.join(project_root, 'public', similarity_mappings_file_name)
    with open(similarity_mappings_file_path, 'r') as sim_mapping_in_file:
        for prod_sim_mapping in sim_mapping_in_file:
            yield prod_sim_mapping


def create_prod_similarity_mappings():
    """
    Creates similarity_mappings for each product and store it to neo4j in the form of relationship (SIMILAR_TO)
    indicating that underlying products are similar to each other.
    :return:
    """
    q = """
    MATCH (prod1:`Product`{product_id: {product_id}})  
    UNWIND {similar_asins} as asin
    MATCH (prod2:`Product`{asin: asin})  
    MERGE (prod1) -[:SIMILAR_TO]-> (prod2)
            """

    prod_sim_mappings_gen = parse_prod_similarity_mappings_file()
    for prod_sim_mappings_json in prod_sim_mappings_gen:
        prod_sim_mappings = json.loads(prod_sim_mappings_json)

        product_id = prod_sim_mappings.keys()[0]
        params = {
            "product_id": product_id,
            "similar_asins": prod_sim_mappings.get(product_id)
        }
        neo4j_con.run(q, params)


def create_products_mapping():
    """
    Creates products mapping in neo4j in the form of relationship (BOUGHT_WITH) indicating that underlying products
    are bought together.
    :return:
    """
    q = """
    MERGE (prod1:`Product`{product_id: {prod_id1}})
    MERGE (prod2:`Product`{product_id: {prod_id2}})
    MERGE (prod1) -[:BOUGHT_WITH]-> (prod2)
        """

    for products_mappings in parse_products_mapping_file():
        params = {
            "prod_id1": products_mappings[0],
            "prod_id2": products_mappings[1]
        }
        neo4j_con.run(q, params)


def parse_products_mapping_file(products_mapping_file_name='amazon0302.txt.gz'):
    """
    Parses the products file and yield it.
    :param products_mapping_file_name: File containing products mappings.
    :return:
    """
    products_mapping_file_path = os.path.join(project_root, 'public', products_mapping_file_name) # 'sample',
    with gzip.open(products_mapping_file_path) as pm_infile:
        for prod_ids in pm_infile:
            if not prod_ids.startswith('#'):
                yield prod_ids[0], prod_ids[1]

if __name__ == '__main__':
    #test_pandas()
    print "Processing and importing data from meta file..."
    parse_create_schema_from_meta_file_using_pandas()

    # print "Processing and importing data from similarity file..."
    # create_prod_similarity_mappings()
    #
    # print "Creating mappings for products which are co-purchased..."
    # create_products_mapping()
