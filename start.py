import json
import pymongo

# Connecting to MongoDB
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['city_inspections']  # database
collection = db['businesses']  # collection

# Path to your JSON file
file_path = "E:\\Files\\BIGDATA\\BigData-proj1\\city_inspections.json"

# Reading the file line by line and inserting into MongoDB
with open(file_path) as file:
    for line in file:
        line = line.strip()  # Remove any leading/trailing whitespace
        if line:  # Check if the line is not empty
            try:
                # Convert the line to a JSON object
                document = json.loads(line)
                
                # Remove unwanted characters
                document_str = json.dumps(document)
                document_str = document_str.replace('$', '')
                document = json.loads(document_str)
                
                # Insert the JSON object into the MongoDB collection
                collection.insert_one(document)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
            except pymongo.errors.PyMongoError as e:
                print(f"MongoDB error: {e}")

print("Data inserted successfully!")

# Question 2

total_businesses = collection.count_documents({})
print(f"Total busineseses: {total_businesses}")

date_query2015 = {"date" : {"$regex": "2015"}}
count2015 = collection.count_documents(date_query2015)
print(f"2015 business count: {count2015}")

date_query2016 = {"date" : {"$regex": "2016"}}
count2016 = collection.count_documents(date_query2016)
print(f"2016 business count: {count2016}")


# Question 3 
business_input = input("Input a business name: ")
business_find = collection.find_one({"business_name":business_input})
if business_find:
    if 'result' in business_find:
        print(business_find['result'])
else:
    print("Business not found.")
# if test:
#      print(f"The date of this {test['business_name']} is {test['date']}")
# else:
#      print("did not work")

client.close()
