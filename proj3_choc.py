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
			cocoa_percent = str(row[4].replace('%',''))
			row[4] = cocoa_percent
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
			statement += '\nORDER BY Bars.Rating'
		elif 'cocoa' in splitted:
			statement += '\nORDER BY Bars.CocoaPercent'
		else:
			statement += '\nORDER BY Bars.Rating'
		
		#Parameter 3
		params3 = ["top", "bottom"]
		if any(c in command for c in params3):
			for x in splitted:
				for y in params3:
					if x.startswith(y):
						if 'top=' in x:
							#print(x[4:])
							statement += ' DESC \nLIMIT "%s"' % (x[4:])
						elif 'bottom=' in x:
							#print(x[7:])
							statement += '\nLIMIT "%s"' % (x[7:])
						else:
							statement += ' DESC \nLIMIT 10'
							#continue
					else:
						continue
		else:
			statement += '\nLIMIT 10'
		
		print(statement)
		cur.execute(statement)
		result = cur.fetchall()
		conn.close()
		return(result)	

	#Companies
	if 'companies' in splitted:
		statement = 'SELECT Bars.Company,Bars.CompanyLocation, COUNT(*)\nFROM Bars\nJOIN Countries ON Bars.CompanyLocationId=Countries.Id'''
		
		#Parameter 1
		params1 = ["country", "region"]
		if any(c in command for c in params1):
			for x in splitted:
				for y in params1:
					if x.startswith(y):
						if 'country=' in x:
							statement += '\nWHERE Countries.Alpha2 = "%s"' % (x[8:])
						elif 'region=' in x:
							statement += '\nWHERE Countries.Region = "%s"' % (x[7:])
						else:
							continue
					else:
						continue

		
		statement += '\nGROUP By Company\nHAVING COUNT(*) > 4'
		
		#Parameter 2
		params2 = ["ratings", "cocoa", "bars_sold"]
		if any(c in command for c in params2):
			for x in splitted:
				for y in params2:
					if x.startswith(y):
						if 'ratings' in x:
							statement = statement.replace('COUNT(*)', 'AVG(Bars.Rating)',1)
							statement += '\nORDER BY AVG(Bars.Rating)'
						elif 'cocoa' in x:
							statement = statement.replace('COUNT(*)', 'AVG(Bars.CocoaPercent)',1)
							statement += '\nORDER BY AVG(Bars.CocoaPercent)'
						elif 'bars_sold' in x:
							statement += '\nORDER BY Count(*)'
						else:
							continue
					else:
						continue
		#Parameter 3
		params3 = ["top", "bottom"]
		if any(c in command for c in params3):
			for x in splitted:
				for y in params3:
					if x.startswith(y):
						if 'top=' in x:
							#print(x[4:])
							statement += ' DESC \nLIMIT "%s"' % (x[4:])
						elif 'bottom=' in x:
							#print(x[7:])
							statement += '\nLIMIT "%s"' % (x[7:])
						else:
							statement += ' DESC \nLIMIT 10'
							#continue
					else:
						continue
		else:
			statement += '\nLIMIT 10'

		print(statement)
		cur.execute(statement)
		result = cur.fetchall()
		conn.close()
		return(result)
	
	#Countries
	if 'countries' in splitted:
		statement = 'SELECT Bars.CompanyLocation, Countries.Region, COUNT(*)\nFROM Bars\nJOIN Countries ON Bars.CompanyLocationId=Countries.Id'''
		
		#Parameter 1
		params1 = ["region"]
		if any(c in command for c in params1):
			for x in splitted:
				for y in params1:
					if x.startswith(y):
						if 'region=' in x:
							statement += '\nWHERE Countries.Region = "%s"' % (x[7:])
						else:
							continue
					else:
						continue
		
		#Parameter 2
		params2 = ["sellers", "sources"]
		if any(c in command for c in params2):
			for x in splitted:
				for y in params2:
					if x.startswith(y):
						if'sellers' in x:
							statement = statement.replace('Bars.CompanyLocation', 'Bars.CompanyLocation',1)
						elif 'sources' in x:
							statement = statement.replace('ON Bars.CompanyLocationId=Countries.Id', 'ON Bars.BroadBeanOrigin=Countries.EnglishName',1)
							statement = statement.replace('Bars.CompanyLocation', 'Bars.BroadBeanOrigin',1)
						else:
							continue
					else:
						continue
			
		statement += '\nGROUP By CompanyLocation\nHAVING COUNT(*) > 4'
		if any(c in command for c in params2):
			for x in splitted:
				for y in params2:
					if x.startswith(y):
						if'sellers' in x:
							statement = statement.replace('GROUP By CompanyLocation', 'GROUP By CompanyLocation',1)
						elif 'sources' in x:
							statement = statement.replace('GROUP By CompanyLocation', 'GROUP By BroadBeanOrigin',1)
						else:
							continue
					else:
						continue

		#Parameter 3
		params3 = ["ratings", "cocoa", "bars_sold"]
		if any(c in command for c in params3):
			for x in splitted:
				for y in params3:
					if x.startswith(y):
						if 'ratings' in x:
							statement = statement.replace('COUNT(*)', 'AVG(Bars.Rating)',1)
							statement += '\nORDER BY AVG(Bars.Rating)'
						elif 'cocoa' in x:
							statement = statement.replace('COUNT(*)', 'AVG(Bars.CocoaPercent)',1)
							statement += '\nORDER BY AVG(Bars.CocoaPercent)'
						elif 'bars_sold' in x:
							statement += '\nORDER BY Count(*)'
						else:
							continue
					else:
						continue
		else:
			statement = statement.replace('COUNT(*)', 'AVG(Bars.Rating)',1)
			statement += '\nORDER BY AVG(Bars.Rating)'
		
		#Parameter 4
		params4 = ["top", "bottom"]
		if any(c in command for c in params4):
			for x in splitted:
				for y in params4:
					if x.startswith(y):
						if 'top=' in x:
							#print(x[4:])
							statement += ' DESC \nLIMIT "%s"' % (x[4:])
						elif 'bottom=' in x:
							#print(x[7:])
							statement += ' ASC \nLIMIT "%s"' % (x[7:])
						else:
							statement += ' DESC \nLIMIT 10'
							#continue
					else:
						continue
		else:
			statement += '\nLIMIT 10'
		print(statement)
		cur.execute(statement)
		result = cur.fetchall()
		conn.close()
		return(result)
	
	#Regions
	if 'regions' in splitted:
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
