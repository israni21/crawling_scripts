import requests
import json
import pandas as pd
from datetime import date, datetime
import pymysql

# Database connection details
DB_HOST = "bytebuzz.cxmmk2uo63v6.ap-south-1.rds.amazonaws.com"
DB_USER = "riya"
DB_PASSWORD = "riya@123"
DB_DATABASE = "krbl_scraping"
DB_PORT = 3306

# Establish a connection to the MySQL server
connection = pymysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    db=DB_DATABASE,
    port=DB_PORT,
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

cursor = connection.cursor()
pf_id = 2

today_date = date.today()
print(today_date)

# Query to get web_pid from the database
query = f"SELECT page_url FROM rb_sku_platform WHERE pf_id = {pf_id}"
cursor.execute(query)
page_url_list = cursor.fetchall()
page_urls = [item['page_url'] for item in page_url_list]

# Fetch pincodes from rb_location table
p_query = f"SELECT pincode FROM rb_location WHERE pf_id = {pf_id}"
cursor.execute(p_query)
pincode_list = cursor.fetchall()
pincodes = [item['pincode'] for item in pincode_list]

# Check which products have already been crawled today
crawled_today_query = f"""
    SELECT web_pid, pincode FROM flipkart_crawl_pdp 
    WHERE pf_id = {pf_id} AND date(crawled_on) = '{today_date}'
"""
cursor.execute(crawled_today_query)
crawled_today = cursor.fetchall()
crawled_set = {(item['web_pid'], item['pincode']) for item in crawled_today}

# Retry configuration
# MAX_RETRIES = 3

# Initialize a list to track failed crawls
failed_crawls = []

# Run the crawling process three times
for attempt in range(1):
    print(f"--- Crawling Attempt {attempt + 1} ---")
    newly_crawled = set()  # Track products successfully crawled in this attempt
    for product_id in page_urls:
        for pin in pincodes:
            pin = str(pin)
            if (product_id, pin) in crawled_set or (product_id, pin) in newly_crawled:
                print(f"Product {product_id} - {pin} already crawled today. Skipping.")
                continue
            print("***")
            headers = {
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Connection': 'keep-alive',
                'Content-Type': 'application/json',
                'Cookie': 'T=TI168854664247200147853618312033957394127285618377572071717900484381; pxcts=18d38dd6-1b10-11ee-9f91-7a555451486f; _pxvid=18d3812e-1b10-11ee-9f91-11505e1b15e0; AMCVS_17EB401053DAF4840A490D4C%40AdobeOrg=1; AMCV_17EB401053DAF4840A490D4C%40AdobeOrg=-227196251%7CMCIDTS%7C19544%7CMCMID%7C20845591984220731522272682155905193845%7CMCAAMLH-1689151445%7C12%7CMCAAMB-1689151445%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1688553845s%7CNONE%7CMCAID%7CNONE; gpv_pn=Product%20Page; gpv_pn_t=GROCERY%3AProduct%20Page; s_cc=true; __pxvid=192720f4-1b10-11ee-b679-0242ac120004; S=d1t16P28/MBA2PyxBPz8/MD9lP9R9TOVieJMaUzjYhLvO1v1zPbLPGSWqSgYogtcxJrwWEQvDylwuFLpgZIWw/1MxOA==; SN=VIA12BC113E27C4B1AA359D11631226727.TOKC3CA8DAF88894AB296C1AFC3E9673E2B.1688546658.LO; _px3=46c2f21c1065c13a611b75062e13fe46e176f9bb813962826905181c32fcfadf:dBJpZ8m4p6xzCqNhMQLnb0V3twqQwnMnakP/aWn1agAawLabMhBkwXzq7uEz8go8Hmq68S1VNMiXH9hiBzJzVA==:1000:6jE2ilMURQpGsgA70mIrB1Qk5Pn2ucnP7sfQcIoViFb/RZXGJRttJCTMxa0VkhyxZH3hFj606R5elV6GSNGgx2a6f8dKFwSLMqQtnmuFVPIEEEcHEjNvwXTRJEvCTGOArSr6ud2EL+C6zljGIx6Psv226sYThARGgkv7GMdbrNS92dRPO3ZOgfigcK55fHp37k80JhK5vWUCPn/pgaBraA==; s_sq=flipkart-prd%3D%2526pid%253DProduct%252520Page%2526pidt%253D1%2526oid%253DfunctionSr%252528%252529%25257B%25257D%2526oidt%253D2%2526ot%253DSPAN',
                'DNT': '1',
                'Origin': 'https://www.flipkart.com',
                'Referer': 'https://www.flipkart.com/',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-site',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
                'X-User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 FKUA/website/42/website/Desktop',
                'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'Accept': 'application/pdf'
            }

            updated_url = product_id
            'https://flipkart.com/abc/p/xyz?pid=BWSESTMUYGXGQYPB&marketplace=GROCERY'
            'https://flipkart.com/abc/p/xyz?pid=MSCD93YTZWRGZ7GQ&lid=LSTMSCD93YTZWRGZ7GQ'

            updated_url = updated_url.split('/')

            desired_url = '/'.join(updated_url[updated_url.index('flipkart.com')+1:])
        
            # retries = 0
            # success = False  
            
            json_data = {
                'pageUri': '/'+ desired_url,
                'locationContext': {
                    'pincode': pin,
                },
                'isReloadRequest': True,
            }
            
            # while retries < MAX_RETRIES and not success:  
            try:
                response = requests.post('https://1.rome.api.flipkart.com/api/4/page/fetch', headers=headers, json=json_data)
                # print(f"Attempt {retries + 1} for Product {product_id} - {pin}: Status {response.status_code}")
                json_response = json.loads(response.text)
                jsn = json_response["RESPONSE"]

                if "pageData" in jsn:
                    data = jsn["pageData"]["pageContext"]
                    page_context = jsn["pageData"]["pageContext"]
                    
                    try:
                        extracted_web_pid = page_context["productId"]
                    except (ValueError, KeyError, TypeError, AttributeError) as err:
                        extracted_web_pid = ""
                        
                    try:
                        title = page_context["titles"]["title"]
                    except (ValueError, KeyError, TypeError, AttributeError) as err:
                        title = ""
                    
                    mrp =  0
                    price = 0
                    s_price = 0
                    try:
                        pricing = page_context["pricing"]
                        
                        currency = pricing["finalPrice"]["currency"]
                        prices = pricing["prices"]
                        price_dict = dict()
                        for pd in prices:
                            key = pd["priceType"]
                            value = pd["value"]
                            if "MRP" in key:
                                mrp = pd["value"]
                                #mrp = prices["value"]
                            elif key == "FSP" and not pd["strikeOff"]:
                                price = pd["value"]
                            elif key == "SPECIAL_PRICE" and not pd["strikeOff"]:
                                price = pd["value"]
                            else:
                                continue
                        
                    except (ValueError, KeyError, TypeError, AttributeError) as err:
                        mrp =  0
                        price = 0
                        currency = "INR"
                    
                    pricing = page_context["pricing"]
                    
                    if pricing is not None:
                        if mrp <= price:
                            mrp = pricing["mrp"]
                            
                        
                        if mrp == 0 and price!=0:
                            mrp = price
                        if price == 0 and mrp!=0:
                            price = mrp
                        if s_price is not None and s_price!=0:
                            price = s_price

                    else:
                        mrp = 0
                        price = 0
                    
                    
                    try:
                        discount = pricing["totalDiscount"]
                    except (ValueError, KeyError, TypeError, AttributeError) as er:
                        discount = 0.0
                    
                    if mrp == 0:
                        if discount != 0.0:
                            mrp = price + (price*discount)/100
                        else:
                            mrp = price
                    savings = mrp-price
                    
                    tracking_data = page_context["trackingDataV2"]
                    
                    sections = list()
                    for key in data.keys():
                        data_array = data[str(key)]
                        try:
                            for sec in data_array:
                                sections.append(sec)
                        except Exception as e:
                            print("")
                    product_detail = None
                    product_specification = None
                    physical_attach = None
                    multimedia = None
                    qna = None
                    highlights = None
                    texts = None
                    availability = None
                    for section in sections:
                        try:
                            if "PRODUCT_SPECIFICATION" in section["elementId"]:
                                product_specification = section
                            elif "PHYSICAL_ATTACH" in section["elementId"]:
                                physical_attach = section
                            elif "PRODUCT_DETAILS" in section["elementId"]:
                                product_detail = section
                            elif "MULTIMEDIA" in section["elementId"]:
                                multimedia = section
                            elif "QNA" in section["elementId"]:
                                qna = section
                            elif "HIGHLIGHTS" in section["elementId"]:
                                highlights = section
                            elif "TEXT" in section["elementId"]:
                                texts = section
                            elif "AVAILABILITY" in section["elementId"]:
                                availability = section
                            else:
                                section=section
                        except (TypeError, ValueError, AttributeError, KeyError) as ke:
                            continue
                        
                    product_status = tracking_data["productStatus"]
                    
                    if availability is not None:
                        announcement = availability["widget"]["data"]["announcementComponent"]["value"]["title"]
                        
                        if "Currently Unavailable" in announcement or "Sold Out" in announcement:
                            product_status = "out of stock"
                        else:
                            product_status = "in stock"
                            
                    
                    announcement2 = page_context["fdpEventTracking"]["events"]["psi"]["pls"]["availabilityStatus"]
                    if "IN_STOCK" in announcement2:
                            product_status = "in stock" 
                    

                    if "current" in product_status:
                        product_status = "in stock"


                    if "out of stock" in product_status.lower():
                        osa = 0
                        osa_remark = "OOS"
                    else:
                        osa = 1
                        osa_remark= "IN STOCK"
                        
                    try:
                        rating_value = page_context["rating"]["average"]
                    except (ValueError, KeyError, TypeError, AttributeError) as err:
                        rating_value = 0
                                    
                    try:
                        rating_count = page_context["rating"]["count"]
                    except (ValueError, KeyError, TypeError, AttributeError) as err:
                        rating_count = 0

                    try:
                        review_count = page_context["rating"]["reviewCount"]
                    except (ValueError, KeyError, TypeError, AttributeError) as err:
                        review_count = 0
                    
                    today_date = date.today()

                    # Insert data into the SQL table
                    insert_query = """
                        INSERT INTO flipkart_crawl_pdp 
                        (pf_id, web_pid, product_title, mrp, price, discount, pincode, osa, osa_remark, rating_value, rating_count, crawled_on)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    date_time = datetime.now() 
                    cursor.execute(insert_query, (
                        pf_id, product_id, title, mrp, price, discount, pin, osa, osa_remark, rating_value, rating_count, date_time
                    ))
                    connection.commit()

                    print(f"*****Product {product_id} - {pin} Crawled and Inserted*****")
                    newly_crawled.add((product_id, pin))
                    success = True
                    
            except Exception as e:
                    # retries += 1
                    # print(f"Error for Product {product_id} - {pin}: {str(e)}")
                    # if retries >= MAX_RETRIES:
                    #     print(f"Failed to crawl Product {product_id} - {pin} after {MAX_RETRIES} attempts.")
                    failed_crawls.append((product_id, pin))

    # Update the crawled set with products successfully crawled in this attempt
    crawled_set.update(newly_crawled)

    print(failed_crawls)
    print(crawled_set)
    # After three attempts, create a DataFrame of failed crawls
    failed_crawls_df = pd.DataFrame(failed_crawls, columns=["web_pid", "pincode"])
    
    # Keep only unique rows in the DataFrame
    failed_crawls_df = failed_crawls_df.drop_duplicates()

# Print the Delta Count and the DataFrame of failed crawls
delta_count = len(failed_crawls)
print(f"Delta Count: {delta_count}")
print("Failed Crawls DataFrame:")
print(failed_crawls_df)             
                    
# Close the database connection
cursor.close()
connection.close()

print("Crawling and Insertion Complete")
