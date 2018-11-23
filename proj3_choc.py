import sqlite3 as sqlite3
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
        conn = sqlite3.connect(DBNAME)
        print(sqlite3.version)
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
    CREATE TABLE `Bars`( 
        `Id` INTEGER PRIMARY KEY AUTOINCREMENT , 
        `Company` TEXT, 
        `SpecificBeanBarName` TEXT, 
        `REF` TEXT, 
        `ReviewDate` TEXT, 
        `CocoaPercent` REAL, 
		'CompanyLocation' TEXT,
        `CompanyLocationId` INTEGER, 
        `Rating` REAL, 
        `BeanType` TEXT, 
		BroadBeanOrigin TEXT, 
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
	conn = sqlite3.connect(DBNAME)
	cur = conn.cursor()

	#[20 points] Read all data from JSON into Countries table
	with open('countries.json') as jsonFile:
		data = json.load(jsonFile)

		for x in data:
			insertion = (None, x['alpha2Code'],x['alpha3Code'], x['name'], x['region'], x['subregion'], x['population'],x['area'])
			statement = 'INSERT INTO "Countries" '
			statement += 'VALUES (?, ?, ?, ?, ?, ?,?,?)'
			cur.execute(statement, insertion)

	#[20 points] Read all data from CSV into Bars table
	with open(BARSCSV) as csvDataFile:
		csvReader = csv.reader(csvDataFile)
		next(csvReader, None)  # skip the headers

		country_ids = cur.execute('SELECT Id, EnglishName from Countries').fetchall()
		'''
		for x in country_ids:
			print(x[1])
		'''
		
		for row in csvReader:
			for x in country_ids:
				if (row[5] == x[1]):
					foreignid_company = x[0]
				if (row[8] == x[1]):
					foreignid_beans = x[0]

			insertion = (None, row[0], row[1], row[2], row[3], row[4], row[5], foreignid_company, row[6], row[7],row[8],foreignid_beans )
			statement = 'INSERT INTO "Bars" '
			statement += 'VALUES (?, ?, ?, ?, ?, ?,?,?,?,?,?,?)'
			cur.execute(statement, insertion)
	
	# Close connection
	conn.commit()
	conn.close()

# Part 2: Implement logic to process user commands
def argument_helper():
	args = argv[1:]
	result = ''

	for arg in args:
		result += " " + arg
	return result

def process_command(command):
	splitted = command.split()

	conn = sqlite3.connect(DBNAME)
	cur = conn.cursor()

	#Bars
	if 'bars' in splitted:
		statement = '''SELECT Bars.SpecificBeanBarName, Bars.Company, Bars.CompanyLocation, Bars.Rating, Bars.CocoaPercent, Bars.BroadBeanOrigin\nFROM Bars\nJOIN Countries ON Bars.CompanyLocationId=Countries.Id'''
		#Parameter 1
		params1 = ["sellcountry", "sourcecountry", "sellregion", "sourceregion"]
		if any(c in command for c in params1):
			for x in splitted:
				for y in params1:
					if x.startswith(y):
						if'sellcountry=' in x:
							#print(x[12:])
							statement += '\nWHERE Countries.Alpha2 = "%s"' % (x[12:])
						elif 'sourcecountry=' in x:
							#print(x[14:])
							statement += '\nWHERE Countries.Alpha2 = "%s"' % (x[14:])
						elif 'sellregion=' in x:
							#print(x[11:])
							statement += '\nWHERE Countries.Region = "%s"' % (x[11:])
						elif 'sourceregion=' in x:
							#print(x[13:])
							statement += '\nWHERE Countries.Region = "%s"' % (x[13:])
						else:
							continue
					else:
						continue
		#Parameter 2
		if 'ratings' in splitted:
			statement += '\nORDER BY Bars.Rating DESC'
		elif 'cocoa' in splitted:
			statement += '\nORDER BY Bars.CocoaPercent DESC'
		else:
			statement += '\nORDER BY Bars.Rating DESC'
		
		#Parameter 3
		if 'top' in command:
			print('top')
		elif 'bottom' in command:
			print('bottom')
		else:
			statement += '\nLIMIT 10'
		
		print(statement)
		cur.execute(statement)
		result = cur.fetchall()
		conn.close()
		return(result)	

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

	result = argument_helper()

	
	executed_command = process_command(result)
	
	for x in executed_command:
		print(x)
	
    #interactive_prompt()
