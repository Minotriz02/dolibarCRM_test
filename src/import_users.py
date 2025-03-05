import json
import requests

# =========================
# CONFIGURACIÓN DE DOLIBARR
# =========================
DOLIBARR_BASE_URL = 'http://localhost/api/index.php'      # Ajusta la URL a tu instancia
DOLIBARR_APIKEY    = 'bbb31213408dda4daf2b7faee0a6ae03c35d7a47'  # Token real de Dolibarr

# =========================
# FUNCIONES AUXILIARES
# =========================

def get_contact_by_mail(mail):
    """
    Busca un contacto en Dolibarr filtrando por email.
    Si no existe, Dolibarr devolverá 404; en ese caso retornamos None.
    """
    url = f"{DOLIBARR_BASE_URL}/contacts/email/{mail}"
    headers = {"DOLAPIKEY": DOLIBARR_APIKEY}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 404:
            # Contacto no encontrado
            return None
        response.raise_for_status()
        return response.json()  # Devuelve el contacto en formato dict
    except requests.RequestException as e:
        print(f"Error al buscar contacto por mail: {e}")
    return None

def create_contact_in_dolibarr(new_contact):
    """
    Crea un nuevo contacto en Dolibarr usando el endpoint POST /contacts.
    Dolibarr suele devolver el ID (entero) directamente en el body de la respuesta.
    """
    url = f"{DOLIBARR_BASE_URL}/contacts"
    headers = {
        "Content-Type": "application/json",
        "DOLAPIKEY": DOLIBARR_APIKEY
    }
    try:
        response = requests.post(url, json=new_contact, headers=headers)
        response.raise_for_status()
        contact_id = response.json()  # Dolibarr devuelve un número (ej: 4)
        print(f"Contacto creado: {new_contact.get('email')} (ID: {contact_id})")
        return contact_id
    except requests.RequestException as e:
        print(f"Error al crear contacto {new_contact.get('email')}: {e}")
        return None

def update_contact_in_dolibarr(contact_id, update_data):
    """
    Actualiza un contacto existente en Dolibarr.
    Se utiliza el método PUT al endpoint /contacts/{id}.
    """
    url = f"{DOLIBARR_BASE_URL}/contacts/{contact_id}"
    headers = {
        "Content-Type": "application/json",
        "DOLAPIKEY": DOLIBARR_APIKEY
    }
    try:
        response = requests.put(url, json=update_data, headers=headers)
        response.raise_for_status()
        print(f"Contacto {contact_id} actualizado con: {update_data}")
        return contact_id
    except requests.RequestException as e:
        print(f"Error al actualizar contacto {contact_id}: {e}")
        return None

def normalize_value(value):
    """
    Convierte el valor a cadena, eliminando espacios extra.
    Si es None, devuelve cadena vacía.
    """
    if value is None:
        return ""
    return str(value).strip()

# =========================
# LÓGICA DE PROCESAMIENTO
# =========================

def process_contact(json_contact, stats):
    """
    Procesa un contacto del archivo JSON:
      - Si ya existe (mismo email), se actualiza solo si hay diferencias.
      - Si no existe, se crea un nuevo contacto.
    """

    # Construimos el cuerpo principal con los campos nativos de Dolibarr
    new_contact_data = {
        "firstname":   json_contact.get("name", ""),
        "lastname":    json_contact.get("last_name", ""),
        "email":       json_contact.get("mail", ""),
        "phone_mobile":json_contact.get("phone_mobile", ""),
        "town":        json_contact.get("primary_address_city", "")
    }

    # Ahora, manejamos los extrafields. Dolibarr espera un subobjeto "array_options"
    # con "options_" + código_del_extrafield como clave.
    clima    = json_contact.get("clima_bulletin", False)
    forecast = json_contact.get("forecast_bulletin", False)
    full_name= json_contact.get("name", "") + " " + json_contact.get("last_name", "")
    city     = json_contact.get("primary_address_city", "")
    new_contact_data["array_options"] = {
        # Asegúrate de que el código del extrafield en Dolibarr sea 'clima_bulletin' y 'forecast_bulletin'
        "options_clima_bulletin":    "1" if clima else "0",
        "options_forecast_bulletin": "1" if forecast else "0",
        "options_full_name":         full_name,
        "options_city":              city
    }

    # Revisamos que el email sea obligatorio para identificar al contacto
    mail = new_contact_data.get("email")
    if not mail:
        print("No se encontró correo, omitiendo contacto.")
        stats["error"] += 1
        return

    # Verificar si el contacto ya existe por email
    existing_contact = get_contact_by_mail(mail)
    if existing_contact:
        # Si existe, determinamos qué campos han cambiado
        differences = {}

        # Comparamos los campos nativos de Dolibarr
        for field in ["firstname", "lastname", "email", "phone_mobile", "town"]:
            new_value = new_contact_data[field]
            old_value = existing_contact.get(field, "")
            if normalize_value(new_value) != normalize_value(old_value):
                differences[field] = new_value

        # Comparamos los extrafields
        old_extrafields = existing_contact.get("array_options", {})
        new_extrafields = new_contact_data["array_options"]
        extrafields_diff = {}
        for exfield_key, exfield_val in new_extrafields.items():
            old_ex_val = old_extrafields.get(exfield_key, "")
            if normalize_value(exfield_val) != normalize_value(old_ex_val):
                extrafields_diff[exfield_key] = exfield_val

        if extrafields_diff:
            differences["array_options"] = extrafields_diff

        # Si hay diferencias, hacemos PUT
        if differences:
            contact_id = existing_contact.get("id")
            updated_id = update_contact_in_dolibarr(contact_id, differences)
            if updated_id:
                stats["updated"] += 1
            else:
                stats["error"] += 1
        else:
            print(f"Contacto con correo {mail} ya está actualizado; no se requiere acción.")
            stats["existing"] += 1

    else:
        # Si no existe, lo creamos con POST
        new_id = create_contact_in_dolibarr(new_contact_data)
        if new_id:
            stats["created"] += 1
        else:
            stats["error"] += 1

# =========================
# FUNCIÓN PRINCIPAL ETL
# =========================

def etl_import_contacts(json_file):
    """
    Lee un archivo JSON con contactos y los inserta/actualiza en Dolibarr.
    """
    stats = {
        "created":  0,
        "updated":  0,
        "existing": 0,
        "error":    0
    }

    with open(json_file, 'r', encoding='utf-8') as file:
        contacts = json.load(file)
        for contact in contacts:
            process_contact(contact, stats)

    print("\nResumen del proceso ETL:")
    print(f"Contactos creados:     {stats['created']}")
    print(f"Contactos actualizados:{stats['updated']}")
    print(f"Contactos sin cambios: {stats['existing']}")
    print(f"Contactos con error:   {stats['error']}")

if __name__ == "__main__":
    etl_import_contacts('users.json')
