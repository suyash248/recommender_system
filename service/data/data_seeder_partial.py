import os, gzip, json, time, csv, uuid
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
        "product_id": -1,
        "title": "NA",
        "reviews": {},
        "similar": {},
        "categories": {}
    }

def random_id():
    return int(uuid.uuid4().int & (1<<24)-1)

def create_indices():
    prod_id_index = "CREATE INDEX ON :Product(product_id)"
    prod_asin_index = "CREATE INDEX ON :Product(asin)"
    category_id_index = "CREATE INDEX ON :Category(id)"
    customer_id_index = "CREATE INDEX ON :Customer(customer_id)"
    customer_num_id_index = "CREATE INDEX ON :Customer(num_id)"

    neo4j_con.run(prod_id_index)
    neo4j_con.run(prod_asin_index)
    neo4j_con.run(category_id_index)
    neo4j_con.run(customer_id_index)
    neo4j_con.run(customer_num_id_index)


def execute_batch(q, params_list):
    print "Executing batch of size : {size} @ {time}".format(size=len(params_list), time=time.asctime())
    tx = neo4j_con.begin()
    for params in params_list:
        tx.append(q, params)
    tx.commit()
    print "Processed a batch of size : {size} @ {time}\n".format(size=len(params_list), time=time.asctime())


def parse_create_schema_from_meta_file_partially(meta_file_name='amazon-meta.txt.gz',
                                                 similarity_mappings_file_name='similarity_mappings.txt', batch_size=500):
    # q = """
    #         MERGE (prod1:`Product`{
    #         categories_count: {categories_count},
    #         asin: {asin}, group: {group}, product_id: {product_id}, title: {title}, avg_rating: {avg_rating},
    #         salesrank: {salesrank}, reviews_total: {reviews_total}, reviews_downloaded: {reviews_downloaded}
    #         })
    #         MERGE (cat1:`Category`{id: {cat}.id, name: {cat}.name})
    #         MERGE (prod1) -[:BELONGS_TO]-> (cat1)
    #         """

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
            MERGE (cust1:`Customer`{customer_id: crd.customer_id}) ON CREATE SET cust1.num_id = crd.num_id 
            MERGE (prod1) -[:`REVIEWED_BY`{rating: crd.rating, votes: crd.votes, helpful: crd.helpful, date: crd.date}]-> (cust1)
            """

    count = sim_count = 0
    params_list = []
    sim_list = []

    meta_file_path = os.path.join(project_root, 'public', meta_file_name)
    similarity_mappings_file_path = os.path.join(project_root, 'public', similarity_mappings_file_name)
    if os.path.exists(similarity_mappings_file_path): os.remove(similarity_mappings_file_path)

    chunksize = 10 ** 6
    reader = pd.read_table(meta_file_path, compression='gzip', sep='\n', index_col=False, header=None, engine='python',
                           chunksize=chunksize)

    prod_meta = new_prod_meta()
    for chunk in reader:
        for lines in chunk.values:
            line = lines[0].strip()

            # print line
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
                        "product_id": int(prod_meta.get('product_id', -1)),
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
                    if count == batch_size:
                        # thread = threading.Thread(target=execute_batch, args=(q, deepcopy(params_list)))
                        # thread.start()
                        execute_batch(q, deepcopy(params_list))
                        params_list = []
                        count = 0
                prod_meta['product_id'] = int(line.split()[1])
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
                    sim_dict = {int(prod_meta['product_id']): sim_prods_asins}
                    oline = json.dumps(sim_dict)
                    sim_list.append(oline)
                    sim_count += 1
                    if sim_count == 100:
                        sim_df = pd.DataFrame(sim_list)
                        sim_df.to_csv(similarity_mappings_file_path, mode='a', index=False, header=False, sep='\n', quoting=csv.QUOTE_NONE)
                        sim_list = []
                        sim_count = 0
                    # oline = json.dumps(sim_dict) + "\n"
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
                    "num_id": random_id(),
                    "rating": int(review_info[4]),
                    "votes": int(review_info[6]),
                    "helpful": int(review_info[8])
                })

    if len(params_list) > 0:
        execute_batch(q, deepcopy(params_list))
        params_list = []
        count = 0

    if len(sim_list) > 0:
        sim_df = pd.DataFrame(sim_list)
        sim_df.to_csv(similarity_mappings_file_path, mode='a', index=False, header=False, sep='\n',
                      quoting=csv.QUOTE_NONE)
        sim_list = []
        sim_count = 0


def parse_and_create_prod_similarity_mappings(similarity_mappings_file_name='similarity_mappings.txt', batch_size=500):
    """
    Parse `similarity_mappings` file and yield each entry.
    :param similarity_mappings_file_name: Name of `similarity_mappings` file
    :return: Generator
    """
    q = """
        MATCH (prod1:`Product`{product_id: {product_id}})  
        UNWIND {similar_asins} as asin
        MATCH (prod2:`Product`{asin: asin})  
        MERGE (prod1) -[:SIMILAR_TO]-> (prod2)
                """
    count = 0
    params_list = []

    similarity_mappings_file_path = os.path.join(project_root, 'public', similarity_mappings_file_name)
    with open(similarity_mappings_file_path, 'r') as sim_mapping_in_file:
        for prod_sim_mappings_json in sim_mapping_in_file:
            prod_sim_mappings = json.loads(prod_sim_mappings_json)

            product_id = int(prod_sim_mappings.keys()[0])
            params = {
                "product_id": product_id,
                "similar_asins": prod_sim_mappings.get(str(product_id))
            }
            params_list.append(params)
            count += 1
            if count == batch_size:
                execute_batch(q, deepcopy(params_list))
                params_list = []
                count = 0

    if len(params_list) > 0:
        execute_batch(q, deepcopy(params_list))
        params_list = []
        count = 0


def parse_and_create_products_mapping_file(products_mapping_file_name='amazon0302.txt.gz', batch_size=500):
    """
   Creates products mapping in neo4j in the form of relationship (BOUGHT_WITH) indicating that underlying products
   are bought together.
   :return:
   """
    q = """
       MATCH (prod1:`Product`{product_id: {prod_id1}})
       MATCH (prod2:`Product`{product_id: {prod_id2}})
       MERGE (prod1) -[:BOUGHT_WITH]-> (prod2)
           """

    params_list = []
    count = 0

    products_mapping_file_path = os.path.join(project_root, 'public', products_mapping_file_name) # 'sample',
    with gzip.open(products_mapping_file_path) as pm_infile:
        for prod_ids_line in pm_infile:
            if not prod_ids_line.startswith('#'):
                prod_ids_line = prod_ids_line.strip()
                prod_ids = prod_ids_line.split("\t")
                params = {
                    "prod_id1": int(prod_ids[0]),
                    "prod_id2": int(prod_ids[1])
                }
                params_list.append(params)
                count += 1
                if count == batch_size:
                    execute_batch(q, deepcopy(params_list))
                    params_list = []
                    count = 0
    if len(params_list) > 0:
        execute_batch(q, deepcopy(params_list))
        params_list = []
        count = 0

if __name__ == '__main__':
    # create_indices()
    # print "Processing and importing data from meta file..."
    # parse_create_schema_from_meta_file_partially(batch_size=1000)
    #
    # print "Processing and importing data from similarity file..."
    # parse_and_create_prod_similarity_mappings(batch_size=1000)

    print "Creating mappings for products which are co-purchased..."
    parse_and_create_products_mapping_file(batch_size=2000)
