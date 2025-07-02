import io
import os
import re
import csv
import sys
import math
import time
import logging
import requests
import pandas as pd
#import tkinter as tk
import tempfile
#from tkinter import messagebox
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (NoSuchElementException, TimeoutException, InvalidSessionIdException)
import pyodbc
from sqlalchemy import create_engine, text
import urllib.parse
import psycopg2
from dotenv import load_dotenv
load_dotenv('variables.env')
from typing import Dict, Optional

def validate_environment_variables() -> Dict[str, str]:
        """
        Valida las variables de entorno requeridas y devuelve un diccionario con los valores validados.        
        Returns:
            Dict[str, str]: Diccionario con las variables de entorno validadas            
        Raises:
            ValueError: Si alguna variable requerida falta o es inválida
        """
        required_vars = {
            'SKOOL_EMAIL': {
                'type': str,
                'validator': lambda x: '@' in x,
                'error_msg': 'Debe ser un email válido'
            },
            'SKOOL_PASSWORD': {
                'type': str,
                'validator': lambda x: len(x) >= 8,
                'error_msg': 'La contraseña debe tener al menos 8 caracteres'
            },
            'DB_NAME': {
                'type': str,
                'validator': lambda x: len(x) > 0,
                'error_msg': 'El nombre de la base de datos no puede estar vacío'
            },
            'DB_USER': {
                'type': str,
                'validator': lambda x: len(x) > 0,
                'error_msg': 'El usuario de la base de datos no puede estar vacío'
            },
            'DB_PASSWORD': {
                'type': str,
                'validator': lambda x: len(x) >= 4,
                'error_msg': 'La contraseña de la base de datos debe tener al menos 4 caracteres'
            },
            'DB_HOST': {
                'type': str,
                'default': 'localhost',
                'validator': lambda x: len(x) > 0,
                'error_msg': 'El host no puede estar vacío'
            },
            'DB_PORT': {
                'type': int,
                'default': 5432,
                'validator': lambda x: 1024 <= x <= 65535,
                'error_msg': 'El puerto debe estar entre 1024 y 65535'
            },
            'NUM_MEMBERS': {
                'type': int,
                'default': 0,
                'validator': lambda x: x >= 0,
                'error_msg': 'El número de miembros debe ser 0 o positivo'
            },
            'DEBUG_MODE': {
                'type': bool,
                'default': False,
                'validator': lambda x: isinstance(x, bool),
                'error_msg': 'Debe ser True o False'
            }
        }

        validated_vars = {}
        for var_name, config in required_vars.items():
            # Obtener el valor de la variable de entorno
            raw_value = os.getenv(var_name)
            
            # Manejar valores por defecto
            if raw_value is None and 'default' in config:
                validated_vars[var_name] = config['default']
                continue
            elif raw_value is None:
                raise ValueError(f"Variable de entorno requerida faltante: {var_name}")
            
            # Convertir al tipo correcto
            try:
                if config['type'] == bool:
                    value = raw_value.lower() == 'true'
                else:
                    value = config['type'](raw_value)
            except (ValueError, TypeError):
                raise ValueError(f"Valor inválido para {var_name}. Se esperaba {config['type'].__name__}")
            
            # Validar el valor
            if not config['validator'](value):
                raise ValueError(f"Valor inválido para {var_name}: {config['error_msg']}")
            
            validated_vars[var_name] = value
        
        return validated_vars





# Configuración global
DEBUG_MODE = os.getenv('DEBUG_MODE', 'false').lower() == 'true'
MEMBERS_PER_PAGE = 30  # Miembros por página en Skool

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

    # Uso en tu aplicación
try:
    env_vars = validate_environment_variables()
    print("Variables de entorno válidas:", env_vars)
except ValueError as e:
    print(f"Error de configuración: {e}")
    sys.exit(1)

class SkoolCoursesScraper:
    """Clase principal para el scraping de miembros en Skool"""

    def __init__(self, total_members=None, external_progress_callback=None):
        self.script_name = os.path.basename(sys.argv[0])
        self.total_members = total_members if total_members is not None else env_vars['NUM_MEMBERS']
        self.progress_callback = external_progress_callback
        self.csv_filename = None
        self.full_path = None
        self.max_page_number = None
        self.page_actual = None
        self.pag_total = None
        self.global_count = 0
        self.start_time = datetime.now()
        self.current_page = 1  # Página actual para el progreso
        self.last_progress = -1

        try:
            self._setup_logging()
            if not self._setup_database_connection():  # Ahora retorna True/False
                self.logger.warning("Conexión a DB fallida, continuando sin DB")
            self._init_chrome_driver()
            self._setup_configuration()
        except Exception as e:
            self.logger.error(f"Error en inicialización: {e}")
            raise
        

    def _setup_logging(self):
        """Configura el sistema de logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('skool_members_scraper.log', encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        

    def _init_chrome_driver(self):
        """Inicializa y configura el ChromeDriver"""
        self.chrome_options = Options()
        self._configure_chrome_options()
        self.service = Service(ChromeDriverManager().install())
        self.driver = None

    def _configure_chrome_options(self):
        """Configura las opciones de Chrome optimizadas"""
        options = [
            "--ignore-certificate-errors",
            "--ignore-ssl-errors",
            "--start-maximized",
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--disable-extensions",
            "--disable-gpu",
            "--headless=new",
            "--disable-blink-features=AutomationControlled",
            "--log-level=3",
            "--disable-logging",
            "--disable-software-rasterizer",  # Evita el fallback a software
            "--disable-gpu-compositing",
            "--disable-webgl",  # Desactiva WebGL si no lo necesitas
            "--enable-features=NetworkService"
        ]

        self.chrome_options = Options()
        for option in options:
            self.chrome_options.add_argument(option)

        self.chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
        self.chrome_options.add_experimental_option('useAutomationExtension', False)

    def _setup_configuration(self):
        """Configuración inicial de URLs y credenciales"""
        self.urls = {
            'login': 'https://www.skool.com/login',
            'members': 'https://www.skool.com/antoecomclub/-/members?t=active'
        }

        self.credentials = {
            'email': os.getenv('SKOOL_EMAIL'),  # variable de entorno
            'password': os.getenv('SKOOL_PASSWORD')  # Reemplaza 
        }

        self.csv_filename, self.full_path = self._generate_unique_filename('Miembros_Skool.csv')
        self.logger.info(f"Inicio del scraping a las {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    def _clean_chrome_processes(self):
        """Limpia procesos residuales de Chrome"""
        try:
            if hasattr(self, 'driver') and self.driver:
                try:
                    # Cierra el navegador de forma controlada
                    self.driver.quit()
                except Exception as e:
                    self.logger.warning(f"Error al cerrar el navegador: {str(e)}")
                
                # Espera para asegurar que se cierre
                time.sleep(1)
            
            # Opcional: Limpiar solo procesos hijos (más seguro)
            if hasattr(self, 'service') and self.service:
                try:
                    self.service.stop()
                except Exception as e:
                    self.logger.warning(f"Error al detener el servicio: {str(e)}")
        except Exception as e:
            self.logger.warning(f"Error en limpieza de procesos: {str(e)}")

    def _wait_for_element(self, by, selector, timeout=15):
        """Espera robusta para elementos"""
        return WebDriverWait(self.driver, timeout).until(
            EC.presence_of_element_located((by, selector))
        )


    

    def _generate_unique_filename(self, base_name):
        """Genera un nombre de archivo único con ruta completa"""
        try:
            # Obtener el directorio actual de trabajo
            current_dir = os.getcwd()

            today = datetime.now().strftime("%d_%m_%Y")
            base_without_ext = os.path.splitext(str(base_name))[0]
            filename_pattern = f"{base_without_ext}_{today}"
            final_filename = f"{filename_pattern}.csv"

            # Combinar directorio con nombre de archivo
            full_path = os.path.join(current_dir, final_filename)
            
            counter = 1
            while os.path.exists(full_path):
                final_filename = f"{filename_pattern}_{counter}.csv"
                full_path = os.path.join(current_dir, final_filename)
                counter += 1

            self.logger.info(f"Archivo de salida: {full_path}")                
            return final_filename, full_path #os.path.abspath(final_filename)
        except Exception as e:
            self.logger.error(f"Error generando nombre de archivo: {e}", exc_info=True)
            return os.path.abspath(base_name)
        

    def _setup_database_connection(self):
        """Configura la conexión a PostgreSQL con mejor manejo de errores"""
        try:
            db_params = {
                'dbname': os.getenv('DB_NAME'),
                'user': os.getenv('DB_USER'),
                'password': os.getenv('DB_PASSWORD'),
                'host': os.getenv('DB_HOST'),
                'port': os.getenv('DB_PORT', '5432')
            }
            
            if None in db_params.values():
                self.logger.warning("Faltan variables de entorno para DB")
                return False
                
            self.sqlalchemy_conn_str = f"postgresql://{db_params['user']}:{urllib.parse.quote_plus(db_params['password'])}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
            self.engine = create_engine(self.sqlalchemy_conn_str)
            
            # Test connection
            with self.engine.connect() as test_conn:
                test_conn.execute(text("SELECT 1"))
                
            self.logger.info("Conexión a DB establecida")
            return True
            
        except Exception as e:
            self.logger.error(f"Error al conectar a DB: {e}")
            self.engine = None
            return False

    def restart_browser(self):
        """Reinicia el navegador sin afectar otras ventanas"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Cierra solo la instancia actual
                if hasattr(self, 'driver') and self.driver:
                    try:
                        self.driver.quit()
                    except Exception as e:
                        self.logger.warning(f"Error al cerrar navegador (intento {attempt + 1}): {e}", exc_info=True)
                
                # Espera para liberar recursos
                time.sleep(1)
                
                # Crea nueva instancia
                self.service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(
                    service=self.service,
                    options=self.chrome_options
                )
                self.driver.set_page_load_timeout(30)
                return True
                
            except Exception as e:
                self.logger.error(f"Intento {attempt + 1} fallido: {str(e)}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 * (attempt + 1))

    def retry_on_failure(max_retries=3, delay=2):
        def decorator(func):
            def wrapper(*args, **kwargs):
                for attempt in range(max_retries):
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        if attempt == max_retries - 1:
                            raise
                        time.sleep(delay * (attempt + 1))
            return wrapper
        return decorator

    @retry_on_failure()

    def login(self):
        """Maneja el proceso de login optimizado"""
        self.logger.info("Iniciando proceso de login")
        
        try:
            self.driver.get(self.urls['login'])
            # Esperar y llenar credenciales
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.ID, 'email'))
            ).send_keys(self.credentials['email'])
            
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.ID, 'password'))
            ).send_keys(self.credentials['password'])
            # Click en submit
            WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, '//button[@type="submit"]'))
            ).click()
            # Esperar redirección
            WebDriverWait(self.driver, 15).until(
                lambda d: d.current_url != self.urls['login'])
            self.logger.info("Login exitoso")
            return True
        except Exception as e:
            self.logger.error(f"Error durante el login: {e}", exc_info=True)
            return False

    def _get_active_member_count(self):
        """Obtiene el número de miembros activos y la última página"""
        try:
            active_button = WebDriverWait(self.driver, 10).until(
                EC.visibility_of_element_located((By.ID, 'chip-filter-chip-active'))
            )
            active_count = int(active_button.text.replace("Active", "").strip())    
            # Obtener número de última página
            pagination = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '[class*="styled__DesktopPaginationControls-sc-4zz1jl-1"]'))
            )
            page_buttons = pagination.find_elements(By.CSS_SELECTOR, 'button[class*="styled__ButtonWrapper-sc-1crx28g-1"]')
            last_page = 1
            
            for button in reversed(page_buttons):
                if button.text.isdigit():
                    last_page = int(button.text)
                    break

            return active_count, last_page
        except Exception as e:
            self.logger.error(f"Error obteniendo conteos: {str(e)}")
            return 0, 1
        
    def _safe_extract(self, by, selector, default):
        """Extrae texto de forma segura con valor por defecto"""
        try:
            element = self.driver.find_element(by, selector)
            return element.text.strip()
        except:
            return default
        
    def _extract_courses_info(self, profile_url):
        """Extrae información de cursos del perfil del miembro optimizado"""
        original_window = self.driver.current_window_handle
        gmail_user = 'NA_Email'
        contribution_member = 'NA_Contrib'

        try:
            self.driver.switch_to.new_window('tab')
            self.driver.get(profile_url)
                
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            # Extraer contribución
            contribution_member = self._safe_extract(
                By.CSS_SELECTOR,
                '[class*="styled__TypographyWrapper-sc-70zmwu-0 fFYLQx"]',
                'NA_Contrib'
            )

            # Extraer email
            try:
                buttons = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'button.styled__DropdownButton-sc-13jov82-9'))
                )
                
                if buttons:
                    buttons[-1].click()  # Click en el último botón de menú
                    
                    membership_settings = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, "//div[contains(text(),'Membership settings')]"))
                    )
                    membership_settings.click()
                    
                    gmail_user = self._safe_extract(
                        By.CSS_SELECTOR,
                        '[class*="styled__MembershipInfo-sc-gmyn28-1 etpmnD"] span',
                        'NA_Email'
                    )
            except Exception as e:
                self.logger.error(f"Error al extraer email: {e}", exc_info=True)

            return gmail_user, contribution_member
        
        except Exception as e:
            self.logger.error(f"Error extrayendo información del perfil: {e}", exc_info=True)
            return gmail_user, contribution_member
        finally:
            try:
                if len(self.driver.window_handles) > 1:
                    self.driver.close()
                self.driver.switch_to.window(original_window)
            except Exception as e:
                self.logger.error(f"Error al cerrar pestaña: {e}", exc_info=True)
                self.restart_browser()


    def _extract_member_info(self, member_text):
        """Extrae información del miembro con asignación inteligente de frase_personal y localizacion"""
        defaults = {
            'Nivel': 'N/A',
            'Miembro': 'N/A',
            'EmailSkool': 'N/A',
            'Activo': 'N/A',
            'Unido': 'N/A',
            'Valor': 'N/A',
            'Contribucion': '0',
            'Renueva': 'N/A',
            'Frase': 'N/A',
            'Localiza': 'N/A',
            'Invito': 'N/A',
            'Invitado': 'N/A'
        }

        try:
            if not member_text or not isinstance(member_text, str):
                return defaults

            # Limpieza inicial manteniendo emojis pero eliminando etiquetas de imágenes
            cleaned_text = re.sub(r'\[IMG\]|#\w+', '', member_text)
            parts = [p.strip() for p in cleaned_text.split('\n') if p.strip() and not any(
                x in p.lower() for x in ['chat', 'membership']
            )]
            
            if not parts:
                return defaults

            # Asignación directa de campos principales
            if len(parts) > 0: defaults['Nivel'] = parts[0]
            if len(parts) > 1: defaults['Miembro'] = parts[1]

            remaining_parts = []
            
            for part in parts[2:]:
                part_lower = part.lower()
                
                # 1. Estado activo (prioridad absoluta para "Online now")
                if 'online now' in part_lower:
                    defaults['Activo'] = 'Online now'
                    continue
                elif 'active' in part_lower:
                    defaults['Activo'] = part.split('Active')[-1].strip() if 'Active' in part else part
                    continue
                    
                # 2. Email Skool (si empieza con @)
                if part.startswith('@'):
                    defaults['EmailSkool'] = part
                    continue
                    
                # 3. Fecha de unión
                if part.startswith('Joined'):
                    defaults['Unido'] = part.replace('Joined', '').strip()
                    continue
                    
                # 4. Valor de membresía
                if part.startswith(('$', '€', '£', 'Free')):
                    defaults['Valor'] = part
                    continue
                    
                # 5. Tiempo de renovación
                if 'renew' in part_lower:
                    days_match = re.search(r'(\d+)\s*days', part)
                    defaults['Renueva'] = f"{days_match.group(1)} days" if days_match else part
                    continue
                    
                # 6. Invitación
                if 'invitó' in part_lower or 'invited by' in part_lower:
                    defaults['Invito'] = part
                    continue
                if 'invitado' in part_lower or 'invited' in part_lower:
                    defaults['Invitado'] = part
                    continue
                    
                remaining_parts.append(part)

            # Procesamiento inteligente de frase_personal vs localizacion
            for part in remaining_parts:
                # Primero verificar si es claramente una localización
                is_location = (
                    # Formatos de dirección típicos
                    re.match(r'^(\w+\s?[#-]\d+\s?[A-Za-z]?|\w+\s\w+,\s\w+)$', part) or
                    # Contiene palabras clave de ubicación
                    any(x in part_lower for x in ['calle', 'avenida', 'av', 'cll', 'carrera', 'cra', 'diagonal', 'dg'])
                )
                
                if defaults['Frase'] == 'N/A':
                    if not is_location and defaults['Localiza'] != 'N/A':
                        # Si no es ubicación y ya tenemos localización, asignar a frase
                        defaults['Frase'] = part
                    else:
                        # Asignar a localización solo si es claramente una dirección
                        if is_location:
                            defaults['Localiza'] = part
                        else:
                            defaults['Frase'] = part
                elif defaults['Localiza'] == 'N/A' and is_location:
                    defaults['Localiza'] = part

            # Post-procesamiento final: si frase sigue N/A pero localizacion tiene valor
            if defaults['Frase'] == 'N/A' and defaults['Localiza'] != 'N/A':
                # Verificar si el valor en localizacion es realmente una frase
                if not any(x in defaults['Localiza'].lower() for x in ['calle', 'av', 'cll', 'cra', '#']):
                    defaults['Frase'] = defaults['Localiza']
                    defaults['Localiza'] = 'N/A'

        except Exception as e:
            self.logger.error(f"Error procesando miembro: {str(e)}", exc_info=True)
            defaults['Error'] = str(e)

        return defaults

    def _parse_fecha_unido(self, fecha_str):
        """Convierte la fecha en formato 'Jun 1, 2025' a objeto datetime"""
        try:
            return datetime.strptime(fecha_str, '%b %d, %Y')
        except ValueError:
            return None

    def _calculate_permanencia(self, fecha_unido_str):
        """Calcula días y meses de permanencia desde la fecha de unión"""
        fecha_unido = self._parse_fecha_unido(fecha_unido_str)
        if not fecha_unido:
            return None, None
        
        hoy = datetime.now()
        delta = hoy - fecha_unido
        
        dias = delta.days
        meses = math.floor(dias / 30)  # Redondeo hacia abajo
        
        return dias, meses


    def navigate_to_members(self):
        """Navega a la página de miembros con manejo de errores"""
        try:
            self.driver.get(self.urls['members'])
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '[class*="styled__MemberItemWrapper-"]'))
            )
            return True
        except Exception as e:
            self.logger.error(f"Error navegando a miembros: {e}", exc_info=True)
            return False
        
    def print_progress(self, current, total):
        if self.progress_callback:
            self.progress_callback(current, total)
            return
            
        progress = int((current / total) * 100)
        if progress != self.last_progress:
            # Barra de progreso visual [=====>   ]
            bar_length = 30
            filled_length = int(bar_length * current // total)
            bar = '=' * filled_length + '>' + ' ' * (bar_length - filled_length - 1)
            
            # Imprimir con salto de línea
            print(f"\rProgreso: {progress}% [{bar}] {current}/{total}\n", end='', flush=True)
            
            self.last_progress = progress
            
            if progress >= 100:
                print("\n¡Proceso completado!")


    def _extract_members_page(self, page_number):
        """Extrae datos de todos los miembros en la página actual con progreso"""
        all_member_data = []
        miembros_procesados = 0

        try:
            members = WebDriverWait(self.driver, 15).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, '[class*="styled__MemberItemWrapper-"]'))
            )
            
            self.logger.info(f"Página {page_number}: Procesando {len(members)} miembros")

            for idx, member in enumerate(members):
                if self.total_members > 0 and self.global_count >= self.total_members:
                    break

                NP = idx + 1
                self.global_count += 1
                nro = self.global_count

                try:
                    member_text = member.text
                    member_info = self._extract_member_info(member_text)

                    #if member_info['EmailSkool'] == 'N/A':                        continue

                    # Calcular permanencia
                    permanencia_dias, permanencia_meses = self._calculate_permanencia(member_info['Unido'])

                    # Procesar perfil para obtener email y contribución
                    profile_link = f'https://www.skool.com/{member_info["EmailSkool"]}?g=antoecomclub'
                    gmail_user, contribution_member = self._extract_courses_info(profile_link)

                    # Crear registro de miembro
                    member_record = (
                        page_number,
                        NP,
                        self.global_count,
                        member_info['Miembro'],
                        member_info['Nivel'],
                        gmail_user,
                        member_info['Activo'],
                        member_info['Unido'],
                        member_info['Valor'],
                        contribution_member,
                        member_info['Renueva'],
                        member_info['EmailSkool'],
                        member_info['Frase'],
                        member_info['Localiza'],
                        member_info['Invito'],
                        member_info['Invitado'],
                        permanencia_dias,
                        permanencia_meses
                    )


                    # Actualizar progresoPermanenciaDias =PermanenciaMeses = 
                   

                    #Unir data
                    all_member_data.append(member_record)
                    #self.global_count += 1
                    miembros_procesados += 1

                    # Actualizar progreso
                    if self.total_members > 0:
                        self.print_progress(self.global_count, self.total_members)
                    else:
                        self.print_progress(page_number, self.pag_total)
                    
                except Exception as e:
                    self.logger.error(f"Error procesando miembro {idx + 1}: {str(e)}")
                    continue

            return all_member_data
        except TimeoutException:
            self.logger.error(f"Timeout: No se encontraron miembros en la página {page_number}")
            return []
        except Exception as e:
            self.logger.error(f"Error crítico en página {page_number}: {str(e)}")
            raise

    def paginate(self):
        """Maneja la paginación a través de todas las páginas con progreso"""
        page_number = 1
        all_data = []

        while True:
            # Extraer datos de la página actual
            page_data = self._extract_members_page(page_number)
            
            if not page_data:
                break
                
            all_data.extend(page_data)
            
            # Guardar datos
            self.save_to_database(page_data)
            self.export_to_csv(page_data, is_first_page=(page_number == 1))
            
            # Verificar si hemos alcanzado el límite de miembros
            if self.total_members > 0 and self.global_count >= self.total_members:
                break
                
            # Intentar pasar a la siguiente página
            try:
                next_button = WebDriverWait(self.driver, 15).until(
                    EC.element_to_be_clickable((By.XPATH, '//button[.//span[contains(text(), "Next")]]'))
                )
                
                # Marcar el último miembro para verificar el cambio de página
                last_member = members[-1] if (members := self.driver.find_elements(
                    By.CSS_SELECTOR, '[class*="styled__MemberItemWrapper-"]')) else None
                
                next_button.click()
                
                # Esperar a que la página cambie
                if last_member:
                    WebDriverWait(self.driver, 15).until(
                        EC.staleness_of(last_member)
                    )
                
                page_number += 1
                self.current_page = page_number
                
            except (NoSuchElementException, TimeoutException):
                self.logger.info("No se encontró el botón 'Next'. Fin de la paginación.")
                break
            except Exception as e:
                self.logger.error(f"Error en paginación: {str(e)}")
                break

        return all_data


    def save_to_database(self, members_data):
        """Guarda los datos de miembros en PostgreSQL usando COALESCE"""
        if not members_data or not hasattr(self, 'connection_string'):
            return False

        try:
            import psycopg2
            conn = psycopg2.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Consulta usando COALESCE para convertir NULL a 0
            query = """
            INSERT INTO miembros_activos_4 (
                pagina, np, numero, nombre_miembro, nivel, email_gmail,
                estado_activo, fecha_unido, valor_membresia, contribucion,
                renueva, email_skool, frase_personal, localizacion, invito, invitado,
                permanencia_dias, permanencia_meses,
                script_ejecutado, archivo_generado, fecha_extraccion
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                COALESCE(%s, 0),  -- permanencia_dias
                COALESCE(%s, 0),  -- permanencia_meses
                %s, %s, CURRENT_TIMESTAMP
            )
            """

            # Agregar el nombre del script a cada registro
            members_data_with_script = [(*member, self.script_name, self.full_path) for member in members_data]
            
            cursor.executemany(query, members_data_with_script)
            conn.commit()
            
            self.logger.info(f"Datos de {len(members_data)} miembros guardados en PostgreSQL")
            return True
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error al guardar en PostgreSQL: {str(e)}", exc_info=True)
            return False
        finally:
            if 'conn' in locals():
                conn.close()

    def export_to_csv(self, members_data, is_first_page=False):
        """Exporta los datos a CSV optimizado"""
        if not members_data:
            return False

        try:
            mode = 'w' if is_first_page else 'a'
            
            with open(self.csv_filename, mode, newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                
                if is_first_page:
                    headers = [
                        "Pag", "NP", "Nro", "Miembro", "Nivel", "Gmail", "Activo", "Unido",
                        "Valor", "Contribuye", "Renueva", "EmailSkool", "Frase",
                        "Localiza", "Invito", "Invitado",                        
                        "PermanenciaDias", "PermanenciaMeses", "ScriptEjecutado", "ArchivoGenerado"    
                    ]
                    writer.writerow(headers)
                
                writer.writerows([(*member, self.script_name) for member in members_data])
            
            self.logger.info(f"CSV actualizado: {self.csv_filename}")
            return True
        except Exception as e:
            self.logger.error(f"Error al exportar CSV: {str(e)}")
            return False
        

    def run(self):
        """Ejecuta el flujo completo del scraper con manejo de errores"""
        try:
            if not self.restart_browser():
                raise Exception("No se pudo iniciar el navegador")
                
            if not self.login():
                raise Exception("No se pudo iniciar sesión")
                
            if not self.navigate_to_members():
                raise Exception("No se pudo navegar a la página de miembros")
            
            # Obtener conteo de miembros y páginas
            active_members, last_page = self._get_active_member_count()
            self.pag_total = last_page
            
            if self.total_members <= 0:
                self.total_members = active_members + 10
                self.logger.info(f"Total de miembros activos detectados: {self.total_members}")
            
            self.logger.info(f"Iniciando scraping de {self.total_members} miembros en {last_page} páginas")   

            # Ejecutar paginación
            self.paginate()
            
        except Exception as e:
            self.logger.error(f"Error en ejecución: {e}", exc_info=True)
            raise
        finally:
            try:
                end_time = datetime.now()
                execution_time = end_time - self.start_time
                self._log_execution_summary(end_time, execution_time)
                self._save_execution_data(end_time, execution_time)
            except Exception as e:
                self.logger.error(f"Error al guardar resultados: {e}", exc_info=True)


    def _log_execution_summary(self, end_time, execution_time):
        """Registra el resumen de la ejecución"""
        self.logger.info("\nResumen de ejecución:")
        

        # Verificación robusta del archivo
        file_exists = False
        for _ in range(5):  # Reintentos
            if os.path.exists(self.csv_filename):
                file_exists = True
                break
            time.sleep(0.5)  # Pequeña pausa
        
        if file_exists:
            self.logger.info(" - Estado: Archivo verificado correctamente")
            print(f"\nProceso completado exitosamente. Archivo generado: {self.csv_filename}")
        else:
            self.logger.warning(" - Advertencia: El archivo no fue encontrado después de varios intentos")
            print("\nProceso completado pero no se encontró archivo generado (puede aparecer con retraso)")

        self.logger.info(f" - Hora de inicio: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f" - Hora de finalización: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f" - Tiempo total: {execution_time}")
        self.logger.info(f" - Miembros procesados: {self.global_count}")
        self.logger.info(f" - Archivo generado: {self.csv_filename}")


    def _save_execution_data(self, end_time, execution_time):
        """Guarda los datos de ejecución en PostgreSQL"""
        try:
            if not hasattr(self, 'engine') or self.engine is None:
                self.logger.warning("No se guardarán datos de ejecución (sin conexión a DB)")
                return

            # Consulta adaptada para SQLAlchemy con PostgreSQL
            insert_query = """
            INSERT INTO scraper_miembros_activos
                (total_miembros_scrapeados, ultima_pagina_scrapeada, hora_inicio,
                hora_fin, tiempo_total, archivo_generado, ultima_ejecucion,
                proxima_ejecucion, estado)
            VALUES
                (:total, :pagina, :inicio, :fin, :tiempo, 
                :archivo, :ultima, :proxima, :estado)
            """

            params = {
                'total': self.global_count,
                'pagina': self.current_page,
                'inicio': self.start_time,
                'fin': end_time,
                'tiempo': str(execution_time),
                'archivo': self.csv_filename,
                'ultima': end_time,
                'proxima': end_time + timedelta(hours=24),
                'estado': 'COMPLETADO' if self.global_count > 0 else 'FALLIDO'
            }

            with self.engine.connect() as connection:
                # Usar text() de SQLAlchemy con parámetros nombrados
                connection.execute(text(insert_query), params)
                connection.commit()

            self.logger.info("Datos de ejecución guardados correctamente en PostgreSQL")
        except Exception as e:
            self.logger.error(f"Error al guardar datos de ejecución en PostgreSQL: {str(e)}", exc_info=True)


if __name__ == "__main__":
    try:
        # Obtener número de miembros desde GUI
        numero_miembros = env_vars['NUM_MEMBERS']
        print(f"Iniciando scraping para {numero_miembros} miembros...")
        
        # Crear y ejecutar scraper
        scraper = SkoolCoursesScraper(total_members=numero_miembros)
        scraper.run()
        
        print("Proceso completado exitosamente")
        sys.exit(0)
    except Exception as e:
        print(f"Error durante la ejecución: {str(e)}")
        sys.exit(1)