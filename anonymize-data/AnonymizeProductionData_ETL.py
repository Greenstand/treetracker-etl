import psycopg2 as db
from faker import Faker
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from random import randrange
from decouple import config

oConnection = db.connect(database=config('EXTRACT_DATABASE'), user=config('EXTRACT_USER'), password=config('EXTRACT_PASS'), host=config('EXTRACT_HOST'), port=config('EXTRACT_PORT'), sslmode='require')
curr = oConnection.cursor()

entityDF = pd.read_sql_query('SELECT * FROM entity', oConnection)

trimmedEntities = entityDF[['id','type','name','first_name','last_name','website','wallet', 'offering_pay_to_plant','logo_url','map_name']]

fake = Faker(['en_US', 'en_GB']) 
Faker.seed(1234)

NumberOrganizations = len(entityDF) 
OrganizationDirectory = [['first_name', 'last_name', 'website', 'logo_url']]
for x in range(NumberOrganizations):
    TempRow = [fake.first_name(), fake.last_name(), fake.url(), fake.image_url()] 
    OrganizationDirectory.append(TempRow)

OrgDirDF = pd.DataFrame(OrganizationDirectory, columns=OrganizationDirectory.pop(0))

uniqueEntities = trimmedEntities.name.unique()

index = 0
while index < len(uniqueEntities):
    if uniqueEntities[index] is None or uniqueEntities[index].isspace() or len(uniqueEntities[index]) == 0:
        uniqueEntities = np.delete(uniqueEntities, index)
    index += 1

NumberEntities = len(uniqueEntities) 
EntitiesDictionary = {}

for x in range(NumberEntities):
    EntitiesDictionary[uniqueEntities[x]] = fake.company()

def anonymizeEntities (row):

    index = row.name
    
    if (row['name'] != None and len(row['name']) > 0 and not row['name'].isspace()):
        row['name'] = EntitiesDictionary[row['name']]
    if (row['first_name'] != None and len(row['first_name']) > 0 and not row['first_name'].isspace()):
        row['first_name'] = OrgDirDF.iloc[index]['first_name']
    if (row['last_name'] != None and len(row['last_name']) > 0 and not row['last_name'].isspace()):
        row['last_name'] = OrgDirDF.iloc[index]['last_name']
    if (row['website'] != None and len(row['website']) > 0 and not row['website'].isspace()):
        row['website'] = OrgDirDF.iloc[index]['website']
    if (row['logo_url'] != None and len(row['logo_url']) > 0 and not row['logo_url'].isspace()):
        row['logo_url'] = OrgDirDF.iloc[index]['logo_url']

    if (row['map_name'] != None and len(row['map_name']) > 0 and not row['map_name'].isspace()):
        row['map_name'] = ''.join(e for e in row['name'] if e.isalnum()).lower() 

    if (row['wallet'] != None and len(row['wallet']) > 0 and not row['wallet'].isspace()):
        row['wallet'] = row['map_name']

    return row

uploadEntities = trimmedEntities.apply(lambda x: anonymizeEntities(x), axis=1)

treesTable = pd.read_sql_query('SELECT * FROM trees WHERE EXTRACT(YEAR FROM time_updated) <> -1 AND active=true;', oConnection, parse_dates=['time_created', 'time_updated'])

uploadTrees = treesTable[['id','time_created','time_updated','missing','priority','cause_of_death_id','planter_id','primary_location_id','settings_id','image_url','certificate_id','lat','lon', 'planter_photo_url','planter_identifier','device_id','note','verified','uuid','approved','status','species_id','planting_organization_id','payment_id','contract_id','token_issued','morphology','age','species','capture_approval_tag','rejection_reason','device_identifier']]

plantersTable = pd.read_sql_query('SELECT * FROM planter', oConnection)

trimPlanters = plantersTable[['id','first_name','last_name','email','organization','image_url','person_id','organization_id']]

def fakeEmailGenerator(firstName, lastName, organization):
    domains = [ "hotmail.com", "gmail.com", "aol.com", "mail.com", "yahoo.com", "live.com"]

    prefix = firstName.lower()[0:1+randrange(len(firstName))]
    suffix = lastName.lower()[0:1+randrange(len(lastName))]
    domain = ""

    domainPick = randrange(len(domains)+1)

    if (domainPick == len(domains) and organization != None and len(organization) > 0 and not organization.isspace()):
        domain = ''.join(e for e in organization if e.isalnum()).lower() + ".com"
    else:
        domainPick = randrange(len(domains))
        domain = domains[domainPick]

    generatedEmail = '{prefix}{suffix}@{domain}'.format(prefix=prefix, suffix=suffix, domain=domain)

    return generatedEmail

NumberPlanters = len(trimPlanters) 
PlanterDirectory = [['first_name', 'last_name', 'image_url']]
for x in range(NumberPlanters):
    TempRow = [fake.first_name(), fake.last_name(), fake.image_url()]
    PlanterDirectory.append(TempRow)

PlanterDF = pd.DataFrame(PlanterDirectory, columns=PlanterDirectory.pop(0))

planterOrgs = trimPlanters.organization.unique()

index = 0
while index < len(planterOrgs):
    if planterOrgs[index] is None or planterOrgs[index].isspace() or len(planterOrgs[index]) == 0:
        planterOrgs = np.delete(planterOrgs, index)
    index += 1

NumberPlanterOrgs = len(planterOrgs) 
PlanterOrgsDictionary = {}

for x in range(NumberPlanterOrgs):
    PlanterOrgsDictionary[planterOrgs[x]] = fake.company()

def anonymizePlanters (row):

    index = row.name

    if (row['first_name'] != None and len(row['first_name']) > 0 and not row['first_name'].isspace()):
        row['first_name'] = PlanterDF.iloc[index]['first_name']
    if (row['last_name'] != None and len(row['last_name']) > 0 and not row['last_name'].isspace()):
        row['last_name'] = PlanterDF.iloc[index]['last_name']
    if (row['organization'] != None and len(row['organization']) > 0 and not row['organization'].isspace()):
        row['organization'] = PlanterOrgsDictionary[row['organization']]
    if (row['email'] != None and len(row['email']) > 0 and not row['email'].isspace()):
        row['email'] = fakeEmailGenerator(row['first_name'], row['last_name'], row['organization'])
    if (row['image_url'] != None and len(row['image_url']) > 0 and not row['image_url'].isspace()):
        row['image_url'] = PlanterDF.iloc[index]['image_url']

    return row

uploadPlanters = trimPlanters.apply(lambda x: anonymizePlanters(x), axis=1)

engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=require'.format(user=config('LOAD_USER'), password=config('LOAD_PASS'), host=config('LOAD_HOST'), port=config('LOAD_PORT'), database=config('LOAD_DATABASE')))

uploadEntities.to_sql('entities', engine, if_exists='replace')

uploadTrees.to_sql('trees', engine, if_exists='replace')

uploadPlanters.to_sql('planters', engine, if_exists='replace')

