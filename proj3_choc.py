import sqlite3 as sqlite
from sqlite3 import Error
import csv
import json
from sys import argv

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

# Creates a database called choc.db
def createDB():

    """ create a database connection to a SQLite database """
    try:
        conn = sqlite.connect(DBNAME)
        print(sqlite.version)
    except Error as e:
        print(e)

    cur = conn.cursor()


    '''
    Drop Tables
    '''
    statement = '''
        DROP TABLE IF EXISTS 'Bars';
    '''
    cur.execute(statement)

    statement = '''
        DROP TABLE IF EXISTS 'Countries';
    '''
    cur.execute(statement)

    conn.commit()

    '''
    Create Tables
    '''

    statement = '''
    CREATE TABLE "Bars" ( 
        `Id` INTEGER PRIMARY KEY AUTOINCREMENT , 
        `Company` TEXT, 
        `SpecificBeanBarName` TEXT, 
        `REF` TEXT, 
        `ReviewDate` TEXT, 
        `CocoaPercent` REAL, 
        `CompanyLocationId` INTEGER, 
        `Rating` REAL, 
        `BeanType` TEXT, 
        BroadBeanOriginId INTEGER, 
        FOREIGN KEY(BroadBeanOriginId) REFERENCES Countries(Id),
        FOREIGN KEY(CompanyLocationId) REFERENCES Countries(Id) 
        )
    '''

	#FOREIGN KEY(`BroadBeanOriginId`) REFERENCES "Countries"(`Id`),
    #FOREIGN KEY(`CompanyLocationId`) REFERENCES "Countries"(`Id`) 
    cur.execute(statement)

    statement = '''
    CREATE TABLE "Countries" ( 
        `Id` INTEGER PRIMARY KEY AUTOINCREMENT , 
        `Alpha2` TEXT, 
        `Alpha3` TEXT, 
        `EnglishName` TEXT, 
        `Region` TEXT, 
        `Subregion` TEXT, 
        `Population` INTEGER, 
        `Area` REAL
        )
    '''
    cur.execute(statement)

    conn.close()

def populate_db():

	# Connect to big10 database
	conn = sqlite.connect(DBNAME)
	cur = conn.cursor()

	#[20 points] Read all data from CSV into Bars table
	with open(BARSCSV) as csvDataFile:
		csvReader = csv.reader(csvDataFile)
		next(csvReader, None)  # skip the headers
		for row in csvReader:
				insertion = (None, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8])
				statement = 'INSERT INTO "Bars" '
				statement += 'VALUES (?, ?, ?, ?, ?, ?,?,?,?,?)'
				cur.execute(statement, insertion)
	
	#[20 points] Read all data from JSON into Countries table
	with open('countries.json') as jsonFile:
		data = json.load(jsonFile)
		for x in data:
			#print(x['name'])
			insertion = (None, x['alpha2Code'],x['alpha3Code'], x['name'], x['region'], x['subregion'], x['population'],x['area'])
			statement = 'INSERT INTO "Countries" '
			statement += 'VALUES (?, ?, ?, ?, ?, ?,?,?)'
			cur.execute(statement, insertion)
	# Close connection
	conn.commit()
	conn.close()

# Part 2: Implement logic to process user commands
def process_command(command):
	if command.split(' ', 1)[0] == 'bar':
		print('bar')

	if command.split(' ', 1)[0]== 'companies':
		print('companies')
	
	if command.split(' ', 1)[0]== 'countries':
		print('countries')

	if command.split(' ', 1)[0]== 'regions':
		print('regions')
	return []


def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')

        if response == 'help':
            print(help_text)
            continue

# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
	createDB()
	populate_db()
	command = argv[1]
	process_command(str(command))
    #interactive_prompt()
