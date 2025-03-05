import requests

# ======================================
# CONFIGURACIÓN DE LA API DE DOLIBARR
# ======================================
DOLIBARR_BASE_URL = 'http://localhost/api/index.php'      # Ajusta la URL a tu instancia
DOLIBARR_API_KEY  = 'bbb31213408dda4daf2b7faee0a6ae03c35d7a47'  # Token real de Dolibarr

# ======================================
# 1. OBTENER CONTACTOS CON clima_bulletin = 1
# ======================================
def get_contacts_with_climabulletin():
    """
    Busca en Dolibarr los contactos cuyo extrafield 'clima_bulletin' esté en '1'.
    Esto asume que en la base de datos se llama options_clima_bulletin.
    """
    # Usamos sqlfilters para filtrar: (t.array_options.options_clima_bulletin:'1')
    url = f"{DOLIBARR_BASE_URL}/contacts?sqlfilters=(te.clima_bulletin:=:'1')"
    headers = {"DOLAPIKEY": DOLIBARR_API_KEY}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        contacts = response.json()  # Dolibarr devuelve un array de contactos
        return contacts
    except requests.RequestException as e:
        print(f"Error al obtener contactos con clima_bulletin=1: {e}")
        return []

# ======================================
# 2. ENVIAR EMAIL A TRAVÉS DEL MÓDULO MAILING
# ======================================
def send_email_via_dolibarr(contact):
    """
    Envía un mailing a un único contacto, creando un mailing individual.
    - Crea la campaña de correo (mailing)
    - Agrega al contacto como destinatario
    - Dispara el envío

    Nota: Esto creará muchos mailings (uno por contacto).
    Si quieres uno solo, debes adaptarlo.
    """
    headers = {
        "Content-Type": "application/json",
        "DOLAPIKEY": DOLIBARR_API_KEY
    }

    # 2.1 Crear el mailing
    create_mailing_url = f"{DOLIBARR_BASE_URL}/mailings"
    mailing_data = {
        "label":   f"Boletín Clima para {contact.get('email', '')}",
        "subject": "Boletín Clima",
        "body":    "Hola, este es tu boletín de clima...",
        "email_from": "no-reply@tudominio.com"
    }
    try:
        r = requests.post(create_mailing_url, json=mailing_data, headers=headers)
        r.raise_for_status()
        mailing_id = r.json()  # Dolibarr suele devolver un entero (ej: 12)
    except requests.RequestException as e:
        print(f"Error al crear mailing para contacto {contact.get('id')}: {e}")
        return False

    # 2.2 Agregar el contacto como destinatario del mailing
    add_receivers_url = f"{DOLIBARR_BASE_URL}/mailings/{mailing_id}/receivers"
    # Para agregar un contacto, usamos contact_ids
    # (Si el contacto no está ligado a un tercero, podría ser necesario un email directo)
    data_receivers = {
        "contact_ids": [contact["id"]]
    }
    try:
        r2 = requests.post(add_receivers_url, json=data_receivers, headers=headers)
        r2.raise_for_status()
    except requests.RequestException as e:
        print(f"Error al agregar contacto {contact.get('id')} como destinatario: {e}")
        return False

    # 2.3 Enviar el mailing
    send_mailing_url = f"{DOLIBARR_BASE_URL}/mailings/{mailing_id}/send"
    try:
        r3 = requests.post(send_mailing_url, headers=headers)
        r3.raise_for_status()
        print(f"Email enviado al contacto ID={contact.get('id')} con mailing ID={mailing_id}")
        return True
    except requests.RequestException as e:
        print(f"Error al enviar mailing {mailing_id} al contacto {contact.get('id')}: {e}")
        return False

# ======================================
# 3. FUNCIÓN PARA ENVIAR BOLETÍN A TODOS
# ======================================
def send_weather_emails():
    """
    Consulta los contactos con clima_bulletin=1 y envía el boletín (vía mailings).
    """
    contacts = get_contacts_with_climabulletin()
    if not contacts:
        print("No se encontraron contactos con clima_bulletin=1.")
        return

    sent_count = 0
    error_count = 0

    for contact in contacts:
        # Cada contacto es un diccionario con campos como: id, email, etc.
        # En Dolibarr, "email" puede estar en contact["email"]
        success = send_email_via_dolibarr(contact)
        if success:
            sent_count += 1
        else:
            error_count += 1

    print("\nResumen del envío de boletines de clima:")
    print(f"Emails enviados correctamente: {sent_count}")
    print(f"Errores de envío: {error_count}")

# ======================================
# 4. FUNCIÓN PRINCIPAL
# ======================================
def send_clima_bulletin():
    """
    Inicia el proceso de envío del boletín de clima vía email (módulo Mailing de Dolibarr).
    """
    print("Iniciando el envío del boletín de clima...")
    send_weather_emails()

if __name__ == "__main__":
    send_clima_bulletin()
