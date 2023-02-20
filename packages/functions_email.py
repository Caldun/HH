import os
import smtplib

import mimetypes                                            # Импорт класса для обработки неизвестных MIME-типов, базирующихся на расширении файла
from email import encoders                                  # Импортируем энкодер
from email.mime.base import MIMEBase  # Общий тип
from email.mime.text import MIMEText                        # Текст/HTML
from email.mime.image import MIMEImage                      # Изображения
from email.mime.audio import MIMEAudio                      # Аудио
from email.mime.multipart import MIMEMultipart

from airflow.models import Variable

from mail_signature import mail_signature                   # Макет подписи


def attach_file(msg, filepath):                             # Функция по добавлению конкретного файла к сообщению
    """
    Функция принимает в качестве аргументов сообщение и путь к файлу. Затем он открывает файл, читает
    его и прикрепляет к сообщению.
    
    :param msg: Сообщение, к которому нужно прикрепить файл
    :param filepath: Путь к файлу, который вы хотите прикрепить
    """

    filename = os.path.basename(filepath)                   # Получаем только имя файла

    ctype, encoding = mimetypes.guess_type(filepath)        # Определяем тип файла на основе его расширения

    if ctype is None or encoding is not None:               # Если тип файла не определяется
        ctype = 'application/octet-stream'                  # Будем использовать общий тип
    maintype, subtype = ctype.split('/', 1)                 # Получаем тип и подтип
    
    if maintype == 'text':                                  # Если текстовый файл
        with open(filepath) as fp:                          # Открываем файл для чтения
            file = MIMEText(fp.read(), _subtype=subtype)    # Используем тип MIMEText
            fp.close()                                      # После использования файл обязательно нужно закрыть
    elif maintype == 'image':                               # Если изображение
        with open(filepath, 'rb') as fp:
            file = MIMEImage(fp.read(), _subtype=subtype)
            fp.close()
    elif maintype == 'audio':                               # Если аудио
        with open(filepath, 'rb') as fp:
            file = MIMEAudio(fp.read(), _subtype=subtype)
            fp.close()
    else:                                                   # Неизвестный тип файла
        with open(filepath, 'rb') as fp:
            file = MIMEBase(maintype, subtype)              # Используем общий MIME-тип
            file.set_payload(fp.read())                     # Добавляем содержимое общего типа (полезную нагрузку)
            fp.close()
            encoders.encode_base64(file)                    # Содержимое должно кодироваться как Base64

    file.add_header('Content-Disposition', 'attachment', filename=filename) # Добавляем заголовки
    msg.attach(file)                                        # Присоединяем файл к сообщению


def process_attachement(msg, files):                        # Функция по обработке списка, добавляемых к сообщению файлов
    """
    Если файл существует, прикрепите его к сообщению. Если файл не существует, но путь есть, то это
    папка, поэтому получите список всех файлов в папке и прикрепите их к сообщению
    
    :param msg: Объект сообщения электронной почты
    :param files: список файлов для прикрепления к письму
    """
    for f in files:
        if os.path.isfile(f):                             # Если файл существует
            attach_file(msg,f)                              # Добавляем файл к сообщению
        elif os.path.exists(f):                  # Если путь не файл и существует, значит - папка
            dir = os.listdir(f)                             # Получаем список файлов в папке
            for file in dir:                                # Перебираем все файлы и...
                attach_file(msg,f+"/"+file)                 # ...добавляем каждый файл к сообщению


def send_email(addr_to: list, subject: str, maket: str, files: list = None)-> bool:
    """
    Он отправляет электронное письмо.
    
    :param addr_to: список получателей
    :param subject: тема письма
    :param maket: HTML-шаблон
    :param files: список файлов для прикрепления к письму
    """

    # переменные для авторизации на почтовом сервере компании
    server = Variable.get("server_mail")
    user = Variable.get("email")
    password = Variable.get("secret_email")

    # Создание составного сообщения и установка заголовка
    msg = MIMEMultipart()
    msg['Subject'] = subject  # формируем заголовок письма
    msg['From'] = '******'  # отправитель
    msg['To'] = ', '.join(addr_to)  # добавляем в строку получателей

     # Формируем html макет письма и присоединяем его к письму
    part_html = MIMEText(maket + mail_signature, 'html', 'utf-8')
    msg.attach(part_html)  # присоединяем макет к письму

    if files == None:
        pass
    else:
        process_attachement(msg, files)

    #======== Этот блок настраивается для каждого почтового провайдера отдельно ===============================================
    server = smtplib.SMTP(server, 587)                      # Создаем объект SMTP
    server.starttls()                                       # Начинаем шифрованный обмен по TLS
    #server.set_debuglevel(True)                            # Включаем режим отладки, если не нужен - можно закомментировать
    server.login(user, password)                            # Получаем доступ
    server.send_message(msg)                                # Отправляем сообщение
    server.quit()                                           # Выходим
    #==========================================================================================================================
    return True
