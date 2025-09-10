from pydantic_settings import BaseSettings


class AuthConfig(BaseSettings):
    JWT_ALG: str = "HS256"
    JWT_SECRET: str = (
        "alkshbd127giasbdqwbdiqssbd89q2gd;oasbd8912bdqbdqwb;qd8912gdbi12gbxiuqbwp9128pbu12b"
    )
    JWT_EXP: int = 60 * 60 * 24 * 60  # 60 days (2 months)

    REFRESH_TOKEN_KEY: str = "refres1o89dhasnd8912nas89hn1o2xn198n2hd98nioxqn8xn91nionhToken"
    REFRESH_TOKEN_EXP: int = 60 * 60 * 24 * 60  # 60 days (2 months)

    SECURE_COOKIES: bool = True


auth_config = AuthConfig() 