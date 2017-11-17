"""
Code for Pipeline Case Study
Author: Jairaj Singh Shaktawat
"""

import sys
from collections import OrderedDict
import pandas as pd
from sqlalchemy import create_engine


def read_input_data():
    """
    This function reads data from the tables and creates a dictionary of
    dataframes with file name as key in the dictionary
    Input: -
    Output: Returns a dictionary of dataframes
    """

    dict_data = {}
    file_names = ['deals.dat', 'deal_investor_relation.dat', 'investor_general.dat']
    file_encodings = ['utf8', 'utf-16', 'utf8']
    df_names = ['deals', 'deal_investor', 'investors']
    for i, j, k in zip(df_names, file_names, file_encodings):
        dict_data[i] = read_dataframes(j, k)
    return dict_data

def read_dataframes(file, encoding):
    """
    This function reads .dat file stores it as a dataframe.
    Input: file name, encoding for that file
    Output: Returns data frame for the file
    """

    try:
        df_in = pd.read_csv(file, encoding=encoding, sep='|')
        return df_in
    except UnicodeDecodeError:
        print("File encoding error: "+file)
        sys.exit(1)
    except FileNotFoundError:
        print("File not found error:"+file)
        sys.exit(1)
    except:
        print(file+" failed to read in dataframe")
        sys.exit(1)


def create_mysql_engine():
    """
    This function creates connection string to connect to mysql database
    Input: -
    Output: Returns Engine object
    """

    flavor = 'mysql+mysqlconnector'
    user = 'root'
    password = 'root'
    host = 'localhost'
    database = 'casestudy'
    string = '''{0}://{1}:{2}@{3}/{4}'''.format(flavor, user, password, host, database)
    try:
        engine = create_engine(string)
        return engine
    except:
        print("Connection string 'flavor' variable for mysql is incorrect")
        sys.exit(1)

def create_mysql_tables(df_name, df_in, engine):
    """
    This function stores the dataframe into mysql database
    Input: dataframe name, dataframe, engine object
    Output: Exception if it fails
    """

    try:
        df_in.to_sql(name=df_name, con=engine, if_exists='replace', index=False)
    except:
        print("Failed connection to database: check user, password and database information")
        sys.exit(1)

def query_data(query, engine):
    """
    This function takes query string & engine and returns resulting dataframe from database
    Input: query string, engine object for mysql
    Output: Returns query result dataframe. Exception if it fails
    """

    try:
        df_result = pd.read_sql(query, con=engine)
        return df_result
    except:
        print("Exception: "+query+"  is incorrect")
        sys.exit(1)

def generate_seedinfo(grouped_company, investor_dict):
    """
    This function takes grouped information and creates a dictionary where the format is:
    companyid: [deal_date,[list of investor id],[list of investor names]]
    Input: company dataframe grouped by comapany id, dictionary of investor_id mapped
           with investor name
    Output: Returns a dictionary that has seed information for all the companies
    """

    dict_company_seedinfo = OrderedDict()
    for company, df_group in grouped_company:
        min_deal_number = df_group['deal_number'].min()
        df_group = df_group[df_group['deal_number'] == min_deal_number]
        if len(df_group) > 0:
            date = df_group['deal_date'].min()
            seed_investor_id = [k for k in df_group["investor_id"]]
            seed_investor_name = [investor_dict[k] for k in df_group["investor_id"]]
            dict_company_seedinfo[company] = [date, seed_investor_id, seed_investor_name]
    return dict_company_seedinfo

def write_output1(foutname, dict_company_seedinfo):
    """
    This function writes seed information for every company in csv file:
    Input: output filename, dictionary that stores seed information for every company
    Output: -
    """

    with open(foutname, 'w') as fout:
        fout.write("Company ID,Seed Date,Seed Investor IDs,Seed Investors\n")
        for company in dict_company_seedinfo:
            seed_date, investor_id, investor_name = dict_company_seedinfo[company]
            # convert above k
            investor_id = ";".join(investor_id)
            investor_name = ";".join(investor_name)
            fout.write('''{0},{1},{2},{3}\n'''.format(company, seed_date, investor_id, investor_name))

def write_output2(foutname, dict_investor_deals, list_investor):
    """
    This function writes Investor to Investor mapping adjacency matrix in a
    csv file where the value is the number of deals both investors took part in
    Input: output filename, dictionary with information of deals for each investor,
            investor id to name mapping dictionary
    Output: -
    """

    header = [""]
    header.extend(list_investor)
    with open(foutname, 'w') as fout:
        fout.write(",".join(header)+"\n")
        # to generate a Matrix
        for i in list_investor:
            temp = [i]
            for j in list_investor:
                if i == j:
                    temp.append("-")
                else:
                    temp.append(str(len(dict_investor_deals[i] & dict_investor_deals[j])))
            fout.write(",".join(temp)+"\n")

def list_seed_investors(engine, dict_investor_id_name):
    """
    This function queries the mysql db to create a dataframe that joins all the 3 tables &
    resulting dataframe is grouped by company id. This dataframe is used to finish all
    further processing in Part 1 of the problem"""

    query = '''SELECT d.company_id, d.deal_number, d.deal_date, e.investor_id
            FROM deals d join deal_investor e
            on d.deal_id = e.deal_id'''
    df_seed_investor = query_data(query, engine)

    #group data by company id
    grouped_company = df_seed_investor.groupby(['company_id'])

    # dictionary to map companyid with seed information
    dict_company_seedinfo = generate_seedinfo(grouped_company, dict_investor_id_name)

    #Write Output 1(seed investors per company)
    print("Now writing part1")
    write_output1("case_study_output1.csv", dict_company_seedinfo)

def adjacency_list_investors(engine, df_investors, dict_investor_id_name):
    """
    This function queries the deal_investor table from mysql db and then group data
    by investor_id. It also creates a dictionary where for each investor value is set
    of all deals the investor was part of. This data is used to perform all the processing
    to finish task 2
    """

    #Query deala and investor data from deal_investor table.

    query = '''SELECT deal_id, investor_id
            FROM deal_investor '''
    df_deals_investor = query_data(query, engine)

    #Group data bt investor_id
    grouped_investors = df_deals_investor.groupby(['investor_id'])

    #Generate a set of all the deals per investor
    dict_investor_deals = {dict_investor_id_name[i] : set(j['deal_id']) for i, j in grouped_investors}

    #Subset list of investors that are involved in atleast 1 deal
    list_investor = []
    for investor in list(df_investors["investor_name"]):
        if investor in dict_investor_deals:
            list_investor.append(investor)

    print("Now writing part2")
    write_output2("case_study_output2.csv", dict_investor_deals, list_investor)

def main():
    """
    Main Function
    This function performs two tasks:
    Task 1: Creates a List of ivestors who were part of seed deal for each company
    Task 2: nXn mapping of investors where value is the number of deals both were part of

    """
    data = read_input_data()
    print("Data read complete")
    engine = create_mysql_engine()
    for key, value in data.items():
        create_mysql_tables(key, value, engine)
    query = '''SELECT investor_id, investor_name FROM investors'''
    df_investors = query_data(query, engine)
    #dictionary to map investor id to investor name
    dict_investor_id_name = dict(zip(df_investors.investor_id, df_investors.investor_name))

    #Part 1
    list_seed_investors(engine, dict_investor_id_name)
    print("Part 1 completed")

    #Part 2
    adjacency_list_investors(engine, df_investors, dict_investor_id_name)
    print("Part 2 completed")

if __name__ == "__main__":
    main()
