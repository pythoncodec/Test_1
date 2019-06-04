import pymongo, json, ast, time, datetime, re
from bson import ObjectId

# CONNECT TO DATABASE
connection = pymongo.MongoClient("localhost", 27017)

# CREATE DATABASE USER
database = connection['mylib']  # my_database
# CREATE COLLECTION 1,2,3,4
collection_1 = database['Collection_1']  # collection 1
collection_2 = database['Collection_2']  # collection 2
collection_3 = database['Collection_3']  # collection_3
collection_4 = database['Collection_4']  # collection_4
collection_5 = database['Collection_5']  # collection_5
print("Database connected")


# To do find the rows of the table
def get_data_collections(collection):
    """
    get document data by document ID
    :return:
    """
    # print(collection)
    data = collection.find()
    return list(data)


# To do insert operation
def insert_mongodb_tables(data1, data2, data3, collection1, collection2, collection3):
    """
    Insert new data or document in collection1,colletion2
    :param data:
    :return:
    """
    collection1.insert_many(data1)
    collection2.insert_many(data2)
    collection3.insert_many(data3)


# Initializing collections into lists
list_1 = get_data_collections(collection_1)
list_2 = get_data_collections(collection_2)

uniquedatelist = []


# removing duplicates in given list of array elements
def unique_list(listvalues):
    return list(dict.fromkeys(listvalues))


# regex =re.compile(capturedTime)
def maxHourDateInDateList(comparingeachdate, uniquedatelist):
    count = 0
    resultant_dates = []
    for comparingeachhour in uniquedatelist:
        count = count + 1
        if re.compile(comparingeachdate).match(comparingeachhour):
            # print(repr(comparingeachhour), "matches")
            matched_date = datetime.datetime.strptime(comparingeachhour, "%Y-%m-%dT%H:%M:%S.%fZ")
            resultant_dates.append(matched_date)
    if len(uniquedatelist) == count:
        # print(max(resultant_dates))
        return format_time(max(resultant_dates))


#  to return dateformat  2019-05-25T13:00:00.000Z
def format_time(t):
    s = datetime.datetime.strftime(t, '%Y-%m-%dT%H:%M:%S.%f')
    tail = s[-7:]
    f = round(float(tail), 3)
    temp = "%.3f" % f
    return "%s%s" % (s[:-7], temp[1:]) + 'Z'


# returns the value of the key
def valueOfKey(list, key):
    for i in range(len(list.keys())):
        if list.keys()[i] == key:
            return list.values()[i]


# Initializing value of the key with parameters
def match_and_insert():
    global values_matched, dates
    if site_item_stime_matches():
        # print "matched"
        values_matched = values_matched + 1
        data_collection_3 = {'site': site1, 'item': item1,
                             'stime': stime1,
                             'counter1': counter1,
                             'counter2': counter2,
                             'counter3': counter3,
                             'counter4': counter4}
        data_collection_3 = ast.literal_eval(json.dumps(data_collection_3))
        traffic_sum = int(data_collection_3.get('counter1')) + int(data_collection_3.get('counter4'))
        data_collection_4 = {'trafficsum': traffic_sum, 'collection': 'collection'.__add__(str(values_matched))}
        data_collection_4.update(data_collection_3)
        data_append1.append(data_collection_3)
        data_append2.append(data_collection_4)

        # To find  unique dates with hours in a given list
        date_append.append(stime1)
        date_substring.append(stime1[0:10])
        uniquedatelist = unique_list(date_substring)
        uniquedatelist_with_hour = unique_list(date_append)
        if (len(list_1) == i - 1):
            # print "matched"
            print(data_append1)

            filtered_dates = []
            count = 0
            data_collection_max_hour_nbh = {}
            for comparing_each_date in uniquedatelist:
                # to find maximum hour in same date
                count = count + 1
                formatted_date = maxHourDateInDateList(comparing_each_date, uniquedatelist_with_hour)

                for nbhvalue in data_append2:
                    if nbhvalue.get('stime') == formatted_date:
                        data_collection_max_hour_nbh.update({formatted_date: nbhvalue.get('trafficsum')})
                        filtered_dates.append({'keys': nbhvalue.get('stime'),
                                               'values': nbhvalue.get('trafficsum')})
                        if len(uniquedatelist) == count:
                            print ((data_collection_max_hour_nbh))
                            print(filtered_dates)
                            insert_mongodb_tables(data_append1, data_append2, filtered_dates, collection_3,
                                                  collection_4, collection_5)


def site_item_stime_matches():
    global site1, site2, item1, item2, stime1, stime2, counter1, counter2, counter3, counter4
    site1 = valueOfKey(list_1[l], 'site')
    site2 = valueOfKey(list_2[k], 'site')
    item1 = valueOfKey(list_1[l], 'item')
    item2 = valueOfKey(list_2[k], 'item')
    stime1 = valueOfKey(list_1[l], 'stime')
    stime2 = valueOfKey(list_2[k], 'stime')
    counter1 = valueOfKey(list_1[l], 'counter1')
    counter2 = valueOfKey(list_1[l], 'counter2')
    counter3 = valueOfKey(list_2[k], 'counter3')
    counter4 = valueOfKey(list_2[k], 'counter4')
    return site1 == site2 \
           and item1 == item2 \
           and stime1 == stime2

#Two lists iterating
if not list_1 == [] and not list_2 == []:
    i = 1
    values_matched = 0
    data_append1 = []
    data_append2 = []
    date_append = []
    date_substring = []
    while (len(list_1) >= i):  # list_1 length
        l = i - 1
        i = 1 + i
        j = 1
        while (len(list_2) >= j):  # list_2 length
            k = j - 1
            j = 1 + j

            # insert if 2 tables matches first 3 fields
            match_and_insert()

# CLOSE DATABASE
connection.close()
