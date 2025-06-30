import requests
from bs4 import BeautifulSoup
import boto3
import uuid
import json

def lambda_handler(event, context):
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return {
                'statusCode': response.status_code,
                'body': json.dumps({'error': 'Error al acceder a la página del IGP'})
            }

        soup = BeautifulSoup(response.content, 'html.parser')

        table = soup.find('table')
        if not table:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'No se encontró la tabla en la página del IGP'})
            }

        headers = [th.text.strip() for th in table.find_all('th')]
        rows = []
        for row in table.find_all('tr')[1:]:  # Saltar encabezado
            cells = row.find_all('td')
            if len(cells) < len(headers):
                continue
            row_data = {headers[i]: cells[i].text.strip() for i in range(len(headers))}
            rows.append(row_data)

        dynamodb = boto3.resource('dynamodb')
        table_dynamo = dynamodb.Table('SismosIGP')

        # Borrar datos previos
        scan = table_dynamo.scan()
        with table_dynamo.batch_writer() as batch:
            for item in scan.get('Items', []):
                batch.delete_item(Key={'id': item['id']})

        # Insertar nuevos
        for i, row in enumerate(rows[:10], start=1):  # Solo los 10 últimos
            row['#'] = i
            row['id'] = str(uuid.uuid4())
            table_dynamo.put_item(Item=row)

        return {
            'statusCode': 200,
            'body': json.dumps(rows[:10], ensure_ascii=False)
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
