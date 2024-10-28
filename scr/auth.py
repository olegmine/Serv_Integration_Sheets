import os
from pathlib import Path
from typing import Optional
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from .logger import logger


class GoogleAuthManager:
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    def __init__(self):
        # Получаем базовую директорию и создаём пути к файлам
        self.base_dir = Path(__file__).parent
        self.config_dir = self.base_dir / "access"
        self.token_path = self.config_dir / "token.json"
        self.credentials_path = self.config_dir / "credentials.json"

        # Создаём директорию если её нет
        self.config_dir.mkdir(parents=True, exist_ok=True)

    async def ensure_credentials_exist(self) -> None:
        """Проверяет наличие файла credentials.json"""
        if not self.credentials_path.exists():
            logger.error(f"Файл credentials.json не найден: {self.credentials_path}")
            raise FileNotFoundError(
                f"Файл credentials.json должен быть размещен в: {self.credentials_path}\n"
                "Получите его в Google Cloud Console -> APIs & Services -> Credentials"
            )

    async def load_existing_credentials(self) -> Optional[Credentials]:
        """Загружает существующие учетные данные из token.json"""
        if self.token_path.exists():
            try:
                logger.debug(f"Загрузка существующего token.json: {self.token_path}")
                return Credentials.from_authorized_user_file(str(self.token_path), self.SCOPES)
            except Exception as e:
                logger.warning(f"Ошибка при загрузке token.json: {e}")
                # Удаляем поврежденный файл
                self.token_path.unlink(missing_ok=True)
        return None

    async def refresh_credentials(self, creds: Credentials) -> Optional[Credentials]:
        """Обновляет просроченные учетные данные"""
        try:
            logger.info("Обновление просроченных учетных данных")
            creds.refresh(Request())
            logger.info("Учетные данные успешно обновлены")
            return creds
        except Exception as e:
            logger.error(f"Ошибка при обновлении учетных данных: {e}")
            return None

    async def get_new_credentials(self) -> Credentials:
        """Получает новые учетные данные через OAuth2"""
        try:
            logger.info("Запуск процесса новой аутентификации")
            flow = InstalledAppFlow.from_client_secrets_file(
                str(self.credentials_path),
                self.SCOPES
            )
            creds = flow.run_local_server(port=0)
            logger.info("Новые учетные данные получены успешно")
            return creds
        except Exception as e:
            logger.error(f"Ошибка при получении новых учетных данных: {e}")
            raise

    async def save_credentials(self, creds: Credentials) -> None:
        """Сохраняет учетные данные в token.json"""
        try:
            logger.debug(f"Сохранение учетных данных в {self.token_path}")
            self.token_path.write_text(creds.to_json())
            logger.info("Учетные данные успешно сохранены")
        except Exception as e:
            logger.error(f"Ошибка при сохранении учетных данных: {e}")
            raise

    async def get_credentials(self) -> Credentials:
        """Основной метод для получения учетных данных"""
        logger.info("Запуск процесса получения учетных данных")

        # Проверяем наличие credentials.json
        await self.ensure_credentials_exist()

        # Пытаемся загрузить существующие учетные данные
        creds = await self.load_existing_credentials()

        # Если учетные данные отсутствуют или недействительны
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                # Пробуем обновить просроченные учетные данные
                creds = await self.refresh_credentials(creds)
            # Если не удалось обновить, получаем новые
            if not creds:
                creds = await self.get_new_credentials()
            # Сохраняем новые или обновленные учетные данные
            await self.save_credentials(creds)

        logger.info("Процесс получения учетных данных завершен успешно")
        return creds


# Пример использования
async def get_credentials():
    auth_manager = GoogleAuthManager()
    return await auth_manager.get_credentials()

