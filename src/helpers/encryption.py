import os
from cryptography.fernet import Fernet

"""
    Why Fernet?
        We do not want to display GTIN to the
        client for multiple business reasons.
        With Fernet we have an easy to use
        symmetric encryption process. Fernet
        guarantees that a message encrypted
        using it cannot be manipulated or read
        without the key.
"""


def _get_secret_key():
    fernet_key = os.environ.get("_FERNET_SECRET_KEY")
    # Transform string to bytestring
    return fernet_key.encode()


def _get_fernet_obj():
    # Instantiate a Fernet object with the secret
    # key from the envoriment.
    return Fernet(_get_secret_key())


############ PUBLIC INTERFACE #############

# INPUT: GTIN as a string
# OUTPUT: Base64 token as a string
def fernet_encrypt(data_string):
    f = _get_fernet_obj()
    # Transform input str to bytestring
    data_bytes = data_string.encode()
    # Encrypt input bytestring with Fernet
    data_bytes_encrypted = f.encrypt_at_time(data_bytes, 0)
    # Return data as string rather then bytestring
    return data_bytes_encrypted.decode()


# INPUT: Encrypted data as base64 Fernet token
# OUTPUT: Encrypted data as string
def fernet_decrypt(encrypted_data):
    f = _get_fernet_obj()
    # Transform input string to bytestring
    encrypted_data_bytes = encrypted_data.encode()
    # Decrypt input bytestring to original data
    decrypted_data_bytes = f.decrypt(encrypted_data_bytes)
    # Return the data as string
    return decrypted_data_bytes.decode()
