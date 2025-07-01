import io
import os
import re
import csv
import sys
import math
import time
import ctypes
import logging
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (NoSuchElementException, TimeoutException)
from dotenv import load_dotenv

load_dotenv('variables.env')

def validate_environment_variables():
    """Valida las variables de entorno requeridas"""
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
        'NUM_MEMBERS': {
            'type': int,
            'default': 0,
            'validator': lambda x: x >= 0,
            'error_msg': 'El número de miembros debe ser 0 o positivo'
        }
    }

    validated_vars = {}
    for var_name, config in required_vars.items():
        raw_value = os.getenv(var_name)
        
        if raw_value is None and 'default' in config:
            validated_vars[var_name] = config['default']
            continue
        elif raw_value is None:
            raise ValueError(f"Variable de entorno requerida faltante: {var_name}")
        
        try:
            value = config['type'](raw_value)
        except (ValueError, TypeError):
            raise ValueError(f"Valor inválido para {var_name}. Se esperaba {config['type'].__name__}")
        
        if not config['validator'](value):
            raise ValueError(f"Valor inválido para {var_name}: {config['error_msg']}")
        
        validated_vars[var_name] = value
    
    return validated_vars

# Configuración global
MEMBERS_PER_PAGE = 30
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

try:
    env_vars = validate_environment_variables()
    print("Variables de entorno válidas:", env_vars)
except ValueError as e:
    print(f"Error de configuración: {e}")
    sys.exit(1)

class SkoolScraper:
    def __init__(self, total_members=None):
        self.script_name = os.path.basename(sys.argv[0])
        self.total_members = total_members if total_members is not None else env_vars['NUM_MEMBERS']
        try:
            self._setup_logging()
            self.logger.info("Inicializando SkoolScraper...")
        except Exception as e:
            print(f"Error configurando logging: {str(e)}")
            raise
        self.csv_filename, self.full_path = self._generate_unique_filename('Miembros_Skool.csv')
        self.global_count = 0
        self.start_time = datetime.now()
        self.current_page = 1
        self.pag_total = 1
        self.last_progress = -1
       
        self._prevent_system_sleep()
        self._init_chrome_driver()
        self._setup_configuration()

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

    def _prevent_system_sleep(self):
        """Previene que el sistema entre en modo de suspensión"""
        self.ES_CONTINUOUS = 0x80000000
        self.ES_SYSTEM_REQUIRED = 0x00000001
        ctypes.windll.kernel32.SetThreadExecutionState(
            self.ES_CONTINUOUS | self.ES_SYSTEM_REQUIRED)

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
            "--disable-logging"
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
            'email': env_vars['SKOOL_EMAIL'],
            'password': env_vars['SKOOL_PASSWORD']
        }

    def _generate_unique_filename(self, base_name):
        """Genera un nombre de archivo único con ruta completa"""
        today = datetime.now().strftime("%d_%m_%Y")
        base_without_ext = os.path.splitext(base_name)[0]
        filename_pattern = f"{base_without_ext}_{today}"
        final_filename = f"{filename_pattern}.csv"
        
        counter = 1
        while os.path.exists(final_filename):
            final_filename = f"{filename_pattern}_{counter}.csv"
            counter += 1

        self.logger.info(f"Archivo de salida: {final_filename}")
        return final_filename, os.path.abspath(final_filename)

    def _wait_for_element(self, by, selector, timeout=15):
        """Espera robusta para elementos"""
        return WebDriverWait(self.driver, timeout).until(
            EC.presence_of_element_located((by, selector))
        )

    def restart_browser(self):
        """Reinicia el navegador"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if hasattr(self, 'driver') and self.driver:
                    self.driver.quit()
                
                time.sleep(1)
                
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

    def login(self):
        """Maneja el proceso de login"""
        self.logger.info("Iniciando proceso de login")
        
        try:
            self.driver.get(self.urls['login'])
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.ID, 'email'))
            ).send_keys(self.credentials['email'])
            
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.ID, 'password'))
            ).send_keys(self.credentials['password'])
            
            WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, '//button[@type="submit"]'))
            ).click()
            
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
        """Extrae información de cursos del perfil del miembro"""
        original_window = self.driver.current_window_handle
        gmail_user = 'NA_Email'
        contribution_member = 'NA_Contrib'

        try:
            self.driver.switch_to.new_window('tab')
            self.driver.get(profile_url)
                
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            contribution_member = self._safe_extract(
                By.CSS_SELECTOR,
                '[class*="styled__TypographyWrapper-sc-70zmwu-0 fFYLQx"]',
                'NA_Contrib'
            )

            try:
                buttons = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'button.styled__DropdownButton-sc-13jov82-9'))
                )
                
                if buttons:
                    buttons[-1].click()
                    
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
        """Extrae información del miembro"""
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

            cleaned_text = re.sub(r'\[IMG\]|#\w+', '', member_text)
            parts = [p.strip() for p in cleaned_text.split('\n') if p.strip() and not any(
                x in p.lower() for x in ['chat', 'membership']
            )]
            
            if not parts:
                return defaults

            if len(parts) > 0: defaults['Nivel'] = parts[0]
            if len(parts) > 1: defaults['Miembro'] = parts[1]

            remaining_parts = []
            
            for part in parts[2:]:
                part_lower = part.lower()
                
                if 'online now' in part_lower:
                    defaults['Activo'] = 'Online now'
                    continue
                elif 'active' in part_lower:
                    defaults['Activo'] = part.split('Active')[-1].strip() if 'Active' in part else part
                    continue
                    
                if part.startswith('@'):
                    defaults['EmailSkool'] = part
                    continue
                    
                if part.startswith('Joined'):
                    defaults['Unido'] = part.replace('Joined', '').strip()
                    continue
                    
                if part.startswith(('$', '€', '£', 'Free')):
                    defaults['Valor'] = part
                    continue
                    
                if 'renew' in part_lower:
                    days_match = re.search(r'(\d+)\s*days', part)
                    defaults['Renueva'] = f"{days_match.group(1)} days" if days_match else part
                    continue
                    
                if 'invitó' in part_lower or 'invited by' in part_lower:
                    defaults['Invito'] = part
                    continue
                if 'invitado' in part_lower or 'invited' in part_lower:
                    defaults['Invitado'] = part
                    continue
                    
                remaining_parts.append(part)

            for part in remaining_parts:
                is_location = (
                    re.match(r'^(\w+\s?[#-]\d+\s?[A-Za-z]?|\w+\s\w+,\s\w+)$', part) or
                    any(x in part_lower for x in ['calle', 'avenida', 'av', 'cll', 'carrera', 'cra', 'diagonal', 'dg'])
                )
                
                if defaults['Frase'] == 'N/A':
                    if not is_location and defaults['Localiza'] != 'N/A':
                        defaults['Frase'] = part
                    else:
                        if is_location:
                            defaults['Localiza'] = part
                        else:
                            defaults['Frase'] = part
                elif defaults['Localiza'] == 'N/A' and is_location:
                    defaults['Localiza'] = part

            if defaults['Frase'] == 'N/A' and defaults['Localiza'] != 'N/A':
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
        meses = math.floor(dias / 30)
        
        return dias, meses

    def navigate_to_members(self):
        """Navega a la página de miembros"""
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
        """Muestra el progreso en la consola"""
        progress = int((current / total) * 100)
        if progress != self.last_progress:
            bar_length = 30
            filled_length = int(bar_length * current // total)
            bar = '=' * filled_length + '>' + ' ' * (bar_length - filled_length - 1)
            
            print(f"\rProgreso: {progress}% [{bar}] {current}/{total}", end='', flush=True)
            
            self.last_progress = progress
            
            if progress >= 100:
                print("\n¡Proceso completado!")

    def _extract_members_page(self, page_number):
        """Extrae datos de todos los miembros en la página actual"""
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

                    permanencia_dias, permanencia_meses = self._calculate_permanencia(member_info['Unido'])

                    profile_link = f'https://www.skool.com/{member_info["EmailSkool"]}?g=antoecomclub'
                    gmail_user, contribution_member = self._extract_courses_info(profile_link)

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

                    all_member_data.append(member_record)
                    miembros_procesados += 1

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
        """Maneja la paginación a través de todas las páginas"""
        page_number = 1
        all_data = []

        while True:
            page_data = self._extract_members_page(page_number)
            
            if not page_data:
                break
                
            all_data.extend(page_data)
            
            self.export_to_csv(page_data, is_first_page=(page_number == 1))
            
            if self.total_members > 0 and self.global_count >= self.total_members:
                break
                
            try:
                next_button = WebDriverWait(self.driver, 15).until(
                    EC.element_to_be_clickable((By.XPATH, '//button[.//span[contains(text(), "Next")]]'))
                )
                
                last_member = members[-1] if (members := self.driver.find_elements(
                    By.CSS_SELECTOR, '[class*="styled__MemberItemWrapper-"]')) else None
                
                next_button.click()
                
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

    def export_to_csv(self, members_data, is_first_page=False):
        """Exporta los datos a CSV"""
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
                        "PermanenciaDias", "PermanenciaMeses"   
                    ]
                    writer.writerow(headers)
                
                writer.writerows(members_data)
            
            self.logger.info(f"CSV actualizado: {self.csv_filename}")
            return True
        except Exception as e:
            self.logger.error(f"Error al exportar CSV: {str(e)}")
            return False

    def run(self):
        """Ejecuta el flujo completo del scraper"""
        try:
            if not self.restart_browser():
                raise Exception("No se pudo iniciar el navegador")
                
            if not self.login():
                raise Exception("No se pudo iniciar sesión")
                
            if not self.navigate_to_members():
                raise Exception("No se pudo navegar a la página de miembros")
            
            active_members, last_page = self._get_active_member_count()
            self.pag_total = last_page
            
            if self.total_members <= 0:
                self.total_members = active_members + 10
                self.logger.info(f"Total de miembros activos detectados: {self.total_members}")
            
            self.logger.info(f"Iniciando scraping de {self.total_members} miembros en {last_page} páginas")   

            self.paginate()
            
        except Exception as e:
            self.logger.error(f"Error en ejecución: {e}", exc_info=True)
            raise
        finally:
            try:
                end_time = datetime.now()
                execution_time = end_time - self.start_time
                self._log_execution_summary(end_time, execution_time)
            except Exception as e:
                self.logger.error(f"Error al guardar resultados: {e}", exc_info=True)
            finally:
                self._cleanup_resources()

    def _log_execution_summary(self, end_time, execution_time):
        """Registra el resumen de la ejecución"""
        self.logger.info("\nResumen de ejecución:")
        
        file_exists = False
        for _ in range(5):
            if os.path.exists(self.csv_filename):
                file_exists = True
                break
            time.sleep(0.5)
        
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

    def _cleanup_resources(self):
        """Limpia los recursos del sistema"""
        try:
            if hasattr(self, 'driver') and self.driver:
                self.driver.quit()
        except Exception as e:
            self.logger.error(f"Error al limpiar recursos: {e}", exc_info=True)
        finally:
            ctypes.windll.kernel32.SetThreadExecutionState(self.ES_CONTINUOUS)

if __name__ == "__main__":
    try:
        numero_miembros = env_vars['NUM_MEMBERS']
        print(f"Iniciando scraping para {numero_miembros} miembros...")
        
        scraper = SkoolScraper(total_members=numero_miembros)
        scraper.run()
        
        print("Proceso completado exitosamente")
        sys.exit(0)
    except Exception as e:
        print(f"Error durante la ejecución: {str(e)}")
        sys.exit(1)
