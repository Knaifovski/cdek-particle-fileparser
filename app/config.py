from pydantic import BaseSettings

import os

class Settings(BaseSettings):
    user: str
    password: str
    shiptor_host: str
    shiptor_api_key: str
    class Config:
        env_file = os.path.join(os.getcwd(),'.env')