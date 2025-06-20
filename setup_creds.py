import keyring
import getpass

SERVICE_NAME = "TeradataUploader"

def setup():
    username = input("Enter your Teradata username: ")
    password = getpass.getpass("Enter your Teradata password: ")

    keyring.set_password(SERVICE_NAME, "username", username)
    keyring.set_password(SERVICE_NAME, "password", password)

    print("Credentials securely stored.")

if __name__ == "__main__":
    setup()
