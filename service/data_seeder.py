import csv, os, gzip, json
from service.neo4jconnector import neo4j_con
from settings import project_root


def parse_meta_file(file_path):
    with gzip.open(file_path) as infile:
        #prods_meta = []
        prod_meta = dict()
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
                            prod_meta['categories']['details']["top"].append((cat_id, cat_name))
                        else:
                            prod_meta['categories']['details']["sub"].append((cat_id, cat_name))

            elif line.startswith("reviews:"):
                prod_meta['reviews'] = prod_meta.get('reviews', {})
                review_stats = line.split()
                prod_meta['reviews']['total'] = int(review_stats[2])
                prod_meta['reviews']['downloaded'] = int(review_stats[4])
                prod_meta['reviews']['helpful'] = float(review_stats[7])
            elif line.find("cutomer:") > -1:
                prod_meta['reviews'] = prod_meta.get('reviews', {})
                prod_meta['reviews']['details'] = prod_meta['reviews'].get("details", [])
                review_info = line.split()
                prod_meta['reviews']['details'].append({
                    "data": review_info[0],
                    "customer_id": review_info[2],  # str(self.customer_map[review_info[2]])
                    "rating": int(review_info[4]),
                    "votes": int(review_info[6]),
                    "helpful": int(review_info[8])
                })
            elif len(line) == 0 and len(prod_meta) > 0:
                yield prod_meta
                #prods_meta.append(prod_meta)
                prod_meta = dict()

        #print json.dumps(prods_meta)


def parse_and_import(file_path, chunk_size = 500):
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
    gen = parse_meta_file(os.path.join(project_root, 'public', 'amazon-meta-small.txt.gz'))
    for pm in gen:
        print json.dumps(pm)
        print "\n---------------------\n"
