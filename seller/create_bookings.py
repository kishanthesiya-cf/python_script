import csv
import json
import requests

# Constants
MERCATUS_API_KEY = 'f45e777f-aa18-4d86-8d92-5ebade6c20c5'
MERCATUS_URL = 'http://mercatus.production.cure.fit.internal/external/partnergroup/create'
OLLIVANDER_AGENT_URL = 'http://ollivander.production.cure.fit.internal/agents/all/v2'
OLLIVANDER_UPDATE_URL = 'http://ollivander.production.cure.fit.internal/agent-attribute/update'
OLLIVANDER_HEADERS = {
    'accept': '*/*',
    'X_TENANT': 'GYMFIT',
    'Content-Type': 'application/json'
}

def create_seller(data):
    headers = {
        'api-key': MERCATUS_API_KEY,
        'Content-Type': 'application/json'
    }
    response = requests.post(MERCATUS_URL, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        return response.json()['sellerId']
    else:
        raise Exception(f"Failed to create seller: {response.text}")

def get_agent_by_email(email):
    params = {
        'inActiveAgentsRequired': 'true',
        'onlyFeaturedRequired': 'false',
        'agentRoomsRequired': 'false',
        'serviceMappingRequired': 'false',
        'agentAttributesRequired': 'false',
        'agentShiftRequired': 'false',
        'agentCentersRequired': 'false',
        'addCountryPrefixToNumber': 'false',
        'agentAssetsRequired': 'false',
        'emailId': email
    }
    response = requests.get(OLLIVANDER_AGENT_URL, headers=OLLIVANDER_HEADERS, params=params)
    if response.status_code == 200:
        agents = response.json()
        if len(agents) == 1:
            return agents[0]['id']
        elif len(agents) > 1:
            raise Exception(f"Multiple agents found for email: {email}")
        else:
            raise Exception(f"No agent found for email: {email}")
    else:
        raise Exception(f"Failed to get agent: {response.text}")

def update_agent_attributes(agent_id, seller_id):
    data = [
        {
            "agentId": agent_id,
            "attributeType": "META",
            "attributeName": "SELLER_ID",
            "attributeValue": seller_id,
            "isActive": True
        },
        {
            "agentId": agent_id,
            "attributeType": "META",
            "attributeName": "IS_SELLER_REGISTERED",
            "attributeValue": "true",
            "isActive": True
        }
    ]
    response = requests.post(OLLIVANDER_UPDATE_URL, headers=OLLIVANDER_HEADERS, data=json.dumps(data))
    if response.status_code != 200:
        raise Exception(f"Failed to update agent attributes: {response.text}")

def process_csv(input_file, output_file):
    with open(input_file, mode='r', encoding='utf-8') as infile, open(output_file, mode='w', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
          = reader.fieldnames + ['status']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            try:
                seller_data = {
                    "name": row['name'],
                    "tenant": "PERSONAL_TRAINER",
                    "onHold": False,
                    "type": "INDIVIDUAL",
                    "sellerName": row['name'],
                    "legalName": row['name'],
                    "isDataComplete": True,
                    "status": "ACTIVE",
                    "contactInfo": {
                        "phoneNumber": row['phone'],
                        "email": row['email']
                    },
                    "address": {
                        "streetAddress": row['streetAddress'],
                        "area": row['area'],
                        "city": row['city'],
                        "state": row['state'],
                        "country": row['country'],
                        "pincode": row['pincode']
                    },
                    "paymentConfig": {
                        "creditPeriod": 1,
                        "settlementFrequency": "FORTNIGHTLY",
                        "commercialModel": "REVENUE_SHARE",
                        "tds": 1,
                        "isGSTRegistrationAvailable": False,
                        "panId": row['pan'],
                        "bankDetails": {
                            "name": row['name'],
                            "ifsc": row['ifsc'],
                            "accountNum": row['accountNum'],
                            "accountName": row['bank']
                        }
                    }
                }

                seller_id = create_seller(seller_data)
                agent_id = get_agent_by_email(row['email'])
                update_agent_attributes(agent_id, seller_id)

                row['status'] = 'Success'
            except Exception as e:
                row['status'] = f'Error: {str(e)}'

            writer.writerow(row)

if __name__ == '__main__':
    process_csv('input.csv', 'output.csv')
