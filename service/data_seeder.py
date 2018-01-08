import csv, os, gzip, json
from service.neo4jconnector import neo4j_con
from settings import project_root


def new_prod_meta():
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

def parse_meta_file(file_path):
    with gzip.open(file_path) as infile:
        #prods_meta = []
        prod_meta = new_prod_meta()
        for line in infile:
            line = line.strip()
            #print line
            if line.startswith("Id:"):
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
                prod_meta['similar'] = {
                    'total': sim_line[1],
                    'asins': sim_line[2:]
                }
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
                prod_meta['reviews']['customers_review_details'] = prod_meta['reviews'].get("customers_review_details", [])
                review_info = line.split()
                prod_meta['reviews']['customers_review_details'].append({
                    "date": review_info[0],
                    "customer_id": review_info[2],  # str(self.customer_map[review_info[2]])
                    "rating": int(review_info[4]),
                    "votes": int(review_info[6]),
                    "helpful": int(review_info[8])
                })
            elif len(line) == 0 and len(prod_meta) > 0:
                yield prod_meta
                #prods_meta.append(prod_meta)
                prod_meta = new_prod_meta()


def create_schema_from_meta_file(meta_file_name='amazon-meta-small.txt.gz'):
    prod_meta_gen = parse_meta_file(os.path.join(project_root, 'public', meta_file_name))
    for prod_meta in prod_meta_gen:
        # print json.dumps(prod_meta)
        # print "\n------------------------------------------------------------------\n"
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
MERGE (prod1) -[:`RATED_BY`{rating: crd.rating, votes: crd.votes, helpful: crd.helpful}]-> (cust1)
"""

        prod_props = {
            "product_id": prod_meta.get('product_id', 'NA'),
            "asin": prod_meta.get('asin', 'NA'),
            "group": prod_meta.get('group', 'NA'),
            "salesrank": prod_meta.get('salesrank', 'NA'),
            "title": prod_meta.get('title', 'NA'),
            "reviews_total": prod_meta['reviews'].get('total', 'NA'),
            "reviews_downloaded": prod_meta['reviews'].get('downloaded', 'NA'),
            "avg_rating": prod_meta['reviews'].get('avg_rating', 'NA'),
            "categories_count": prod_meta["categories"].get("count", 'NA')
        }

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
        neo4j_con.run(q, params)

def parse_and_import_products_mapping(file_path, chunk_size = 500):
    products_reader = list(csv.reader(open(file_path, 'rb'), delimiter='\t'))
    params = []
    count = 0

    stmt = """UNWIND {datas} AS data 
MERGE (prod1:`Product`{id: data.prod_id1})
MERGE (prod2:`Product`{id: data.prod_id2})
MERGE (prod1) -[:BOUGHT_WITH]-> (prod2)
    """
    print "Preparing data from file...", file_path
    for prod_ids in products_reader:
        prod_id1, prod_id2 = prod_ids[0], prod_ids[1]
        params.append({"prod_id1": prod_id1, "prod_id2": prod_id2})
        count += 1
        if count % chunk_size == 0:
            neo4j_con.run(stmt, {"datas": params})
            print "Imported {} record(s) successfully!".format(len(params))
            count = 0
            params = []

    neo4j_con.run(stmt, {"datas": params})
    print "Imported {} record(s) successfully!".format(len(params))


if __name__ == '__main__':
    #file_path = os.path.join(project_root, 'public', 'amazon0302.txt')
    #parse_and_import(file_path)
    create_schema_from_meta_file()
