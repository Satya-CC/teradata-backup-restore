import teradatasql
import keyring

SERVICE_NAME = "Teradata_Backup_Engine"

def get_connection():
    username = keyring.get_password(SERVICE_NAME, "username")
    password = keyring.get_password(SERVICE_NAME, "password")
    host='TDPG'
    logmech='LDAP'

    if not username or not password:
        raise Exception("Teradata credentials not found. Run setup_credentials.py to store them.")
    
    try:
        conn = teradatasql.connect(
            host=host,
            user=username,
            password=password,
            logmech=logmech
        )
        print("✅ Connection successful!")
        return conn  # ✅ return the live connection object

    except Exception as e:
        print("❌ Connection failed:")
        print(e)
        return None