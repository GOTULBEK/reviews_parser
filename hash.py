import hashlib
from passlib.context import CryptContext

# 1. Initialize the bcrypt hasher
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# 2. Define your long password
password = "Secret123!"

# 3. Pre-hash with SHA-256 to bypass the 72-byte limit
pre_hashed = hashlib.sha256(password.encode('utf-8')).hexdigest()

# 4. Hash with bcrypt and print
hashed_password = pwd_context.hash(pre_hashed)
print(hashed_password)