import requests
import subprocess
import copy
from datetime import datetime

# Configuración de la API de Mautic
MAUTIC_BASE_URL = 'http://localhost:8080'  # URL de tu instancia de Mautic
MAUTIC_USERNAME = 'mautic'                 # Usuario configurado para la API en Mautic
MAUTIC_PASSWORD = 'Khiara1919;'            # Contraseña para la API

# ID de los templates configurados en Mautic
EMAIL_TEMPLATE_ID = 1   
SMS_TEMPLATE_ID   = 1  
CAMPAIGN_ORIGINAL_ID = 1   
BASE_CAMPAIGN_NAME = "Campaña Clonada - Envío recurrente"

def get_contacts_with_climabulletin():
    """
    Obtiene los contactos que tienen el campo 'climabulletin' establecido en verdadero.
    Se utiliza el parámetro 'search' de la API para filtrar.
    """
    url = f"{MAUTIC_BASE_URL}/api/contacts?search=climabulletin:1"
    response = requests.get(url, auth=(MAUTIC_USERNAME, MAUTIC_PASSWORD))
    contacts = []
    if response.status_code == 200:
        data = response.json()
        contacts = list(data.get("contacts", {}).values())
    else:
        print(f"Error al obtener contactos: {response.text}")
    return contacts

def send_email_via_mautic(contact_id, email_template_id=EMAIL_TEMPLATE_ID):
    """
    Envía el boletín de clima a un contacto mediante el template de email configurado en Mautic.
    """
    url = f"{MAUTIC_BASE_URL}/api/emails/{email_template_id}/contact/{contact_id}/send"
    response = requests.post(url, auth=(MAUTIC_USERNAME, MAUTIC_PASSWORD))
    if response.status_code == 200:
        print(f"Email enviado al contacto {contact_id}")
        return True
    else:
        print(f"Error al enviar email al contacto {contact_id}: {response.text}")
        return False

def send_weather_emails():
    """
    Consulta los contactos que tienen activado el boletín de clima y les envía el email.
    """
    contacts = get_contacts_with_climabulletin()
    sent_count = 0
    error_count = 0

    if not contacts:
        print("No se encontraron contactos con boletín de clima activado para email.")
        return

    for contact in contacts:
        contact_id = contact.get("id")
        if send_email_via_mautic(contact_id):
            sent_count += 1
        else:
            error_count += 1

    print("\nResumen del envío de boletines por Email:")
    print(f"Emails enviados: {sent_count}")
    print(f"Errores: {error_count}")

def send_sms_via_mautic(contact_id, sms_template_id=SMS_TEMPLATE_ID):
    """
    Envía un SMS a un contacto mediante el template SMS configurado en Mautic.
    """
    url = f"{MAUTIC_BASE_URL}/api/smses/{sms_template_id}/contact/{contact_id}/send"
    response = requests.get(url, auth=(MAUTIC_USERNAME, MAUTIC_PASSWORD))
    if response.status_code == 200:
        print(f"SMS enviado al contacto {contact_id}")
        return True
    else:
        print(f"Error al enviar SMS al contacto {contact_id}: {response.text}")
        return False

def send_sms_notifications():
    """
    Consulta los contactos que tienen activado el boletín de clima y les envía un SMS.
    """
    contacts = get_contacts_with_climabulletin()
    sent_count = 0
    error_count = 0

    if not contacts:
        print("No se encontraron contactos con boletín de clima activado para SMS.")
        return

    for contact in contacts:
        contact_id = contact.get("id")
        if send_sms_via_mautic(contact_id):
            sent_count += 1
        else:
            error_count += 1

    print("\nResumen del envío de boletines por SMS:")
    print(f"SMS enviados: {sent_count}")
    print(f"Errores: {error_count}")

def get_campaign(campaign_id):
    """
    Obtiene la configuración de la campaña original.
    """
    url = f"{MAUTIC_BASE_URL}/api/campaigns/{campaign_id}"
    response = requests.get(url, auth=(MAUTIC_USERNAME, MAUTIC_PASSWORD))
    if response.status_code == 200:
        return response.json()  # Se espera que la respuesta tenga la configuración de la campaña
    else:
        print(f"Error al obtener la campaña {campaign_id}: {response.text}")
        return None

def clone_campaign(original_campaign_id, base_campaign_name):
    """
    Clona la campaña original con un nuevo nombre que incluye la hora actual.
    Nota: Es posible que debas ajustar la estructura del JSON según tu versión de Mautic.
    """
    original_data = get_campaign(original_campaign_id)
    if not original_data:
        print("No se pudo obtener la campaña original.")
        return None

    # Copia la configuración de la campaña
    new_campaign_data = copy.deepcopy(original_data.get("campaign", {}))

    # Quitar campos que no se deben clonar
    new_campaign_data.pop("id", None)
    new_campaign_data.pop("dateAdded", None)
    new_campaign_data.pop("dateModified", None)
    
    # Asigna un nuevo nombre incluyendo la hora actual
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    new_campaign_data["name"] = f"{base_campaign_name} - {current_time}"
    
    # Endpoint para crear una nueva campaña (verifica en tu Mautic la ruta correcta)
    url = f"{MAUTIC_BASE_URL}/api/campaigns/new"
    response = requests.post(url, json=new_campaign_data, auth=(MAUTIC_USERNAME, MAUTIC_PASSWORD))
    if response.status_code in [200, 201]:
        print("Campaña clonada exitosamente.")
        return response.json()
    else:
        print(f"Error al clonar la campaña: {response.text}")
        return None

def trigger_campaigns():
    """
    Ejecuta los comandos CLI de Mautic para actualizar segmentos, campañas y disparar las campañas.
    Es necesario que el script tenga acceso al comando docker y que el contenedor de Mautic se llame 'mautic'.
    """
    try:
        print("Actualizando segmentos...")
        subprocess.run(["docker", "exec", "mautic", "php", "/var/www/html/bin/console", "mautic:segments:update"], check=True)
        print("Actualizando campañas...")
        subprocess.run(["docker", "exec", "mautic", "php", "/var/www/html/bin/console", "mautic:campaigns:update"], check=True)
        print("Disparando campañas...")
        subprocess.run(["docker", "exec", "mautic", "php", "/var/www/html/bin/console", "mautic:campaigns:trigger"], check=True)
        print("Campañas disparadas con éxito.")
    except subprocess.CalledProcessError as e:
        print(f"Error al ejecutar comando: {e}")

def send_clima_bulletin():
    """
    Ejecuta el envío del boletín de clima vía email y SMS, y luego dispara la campaña de Mautic.
    """
    print("Iniciando el envío del boletín de clima...")
    print("\nEnviando boletín por Email:")
    send_weather_emails()
    print("\nEnviando boletín por SMS:")
    send_sms_notifications()
    print("\nCreando campaña de boletin de clima...")
    clone_campaign(CAMPAIGN_ORIGINAL_ID, BASE_CAMPAIGN_NAME)
    print("\nEjecutando comandos de campaña en Mautic...")
    trigger_campaigns()

if __name__ == "__main__":
    send_clima_bulletin()
