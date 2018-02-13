#!/usr/bin/python
# -*- coding: utf-8 -*-

###############################################################################
# Сервер опроса складов
###############################################################################

# Форкаемся и бросаем терминал
import sys
import os
import time
import re
import errno
import socket
import select
import logging, logging.handlers
import threading
from BaseHTTPServer import BaseHTTPRequestHandler # для разбора запроса

class Feeler:
        def __init__(self,stock,request):
                self.stock=stock
                self.request=request
                # сформировать текст запроса
                self.c="GET " + self.request.path + " HTTP/1.1\r\n"
                self.c+="Accept-Charset: UTF-8\r\n"
                self.c+="User-Agent: DiPHOST/TEST 0.1\r\n"
                self.c+="Host: " + self.stock + "\r\n"
                self.c+="Connection: close\r\n"
                self.c+="\r\n"
                self.c=str(self.c)
                # длина запроса
                self.total=len(self.c)
                # флажки и времена
                self.http_code=-1
                # содержимое ответа
                self.contents = ''
                self.content_length = 0
                self.headers = ''
                self.response = ''
                # технологическая переменная для счётчика отправленных байтов
                self.next=0

class HTTPRequest(BaseHTTPRequestHandler):
        def __init__(self, rfile):
                self.rfile = rfile
                self.raw_requestline = self.rfile.readline()
                self.error_code = self.error_message = None
                self.parse_request()

class HandleClient(threading.Thread):
        def __init__(self,connection,stocks,rt):
                threading.Thread.__init__(self)
                self.connection=connection
                self.stocks=stocks
                self.receive_timeout=rt
                self.time=time.time()
                self.stop=0
                self.ready=[]

        def run(self):
                logger.debug("[=] (%s) Старт обработчика запроса", self.name)
                self.connection.settimeout(30)
                try:
                        rfile = self.connection.makefile('rb', 8)
                        # Разобрать запрос
                        request = HTTPRequest(rfile)
                        logger.debug("[=] (%s) Запрос: %s", self.name, request.path)
                        self.harvest(request)
                except:
                        logger.error("[!] (%s) Аварийное завершение опроса %s", self.name, sys.exc_value)
                finally:
                        logger.debug("[+] (%s) Завершена работа обработчика входящего соединения", self.name)
                        try:
                                self.connection.close()
                        except:
                                self.stop=1
                                logger.critical("[!] (%s) Обрыв связи %s", self.name, sys.exc_value)
                        return

        def harvest(self,request):
                networker = Harvester( self.stocks, request, self, self.receive_timeout )
                t1 = time.time()
                networker.start()
                # вывести, если чего скопилось
                self.connection.sendall("HTTP/1.1 200 Ok\r\nContent-type: text/plain\r\n\r\n")
                while 1:
                        if self.ready:
                                while self.ready:
                                        p=self.ready.pop(0)
                                        try:
                                                result=p.contents
                                                msg = p.stock
                                                if p.headers:
                                                    r = re.search('Location:\s+https\:\/\/(.*)$', p.headers, re.I+re.M)
                                                    if r:
                                                            loc = r.group(1)
                                                            logger.debug("[+] (%s) РЕЗУЛЬТАТ (склад: %s): переход на HTTPS: https://%s", self.name, p.stock, loc)
                                                            msg += " relocated to: %s\r\n" % loc
                                                    else:
                                                            logger.debug("[+] (%s) РЕЗУЛЬТАТ (склад: %s): НЕТ перехода на HTTPS", self.name, p.stock)
                                                            msg += " not relocated\r\n"
                                                else:
                                                    logger.debug("[+] (%s) РЕЗУЛЬТАТ (склад: %s): НЕТ заголовков", self.name, p.stock)
                                                    msg += " is headerless\r\n"
                                                self.connection.sendall(msg)
                                        except:
                                                logger.debug("[+] (%s) Что-то пошло не так: %s", self.name, sys.exc_value)
                                        if networker.isAlive():
                                                time.sleep(0.001)
                        elif networker.isAlive():
                                time.sleep(0.1)
                        else:
                                break
                networker.join()
                t2=time.time()
                logger.debug("Завершение обработчика. Время %.2f сек", t2-self.time)
                self.stop=1

        def ready_output(self, stock):
                self.ready.append( stock )

class Harvester(threading.Thread):
        def __init__(self, actualstocks, request, processer, receive_timeout):
                threading.Thread.__init__(self)
                self.actualstocks = actualstocks
                self.request = request
                self.receive_timeout = receive_timeout
                self.processer = processer

        def run(self):
                t1=time.time()
                threads={}
                sockets=[]
                connecters=[]
                error_stocks=[]
                for stock in self.actualstocks:
                        try:
                                p = Feeler(stock,self.request)
                                sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                                sock.settimeout(0)
                                threads[sock]=p
                                sockets.append(sock)
                                connecters.append(sock)
                        except:
                                logger.error("[!] (%s) Сокет для склада %s не создался: %s", self.name, stock, sys.exc_value)
                # Опросить
                writers=[]
                readers=[]
                ready=[]
                cnt=0
                selectblocktime=0
                while 1:
                        t2=time.time()-t1
                        #Соединиться, если ещё е совсеми попытались
                        if connecters:
                                logger.debug("[=] (%s) Начинаем соединяться", self.name)
                                while connecters:
                                        sock=connecters.pop(0)
                                        f=threads[sock]
                                        try:
                                                sock.connect((f.stock, 80))
                                                writers.append(sock)
                                        except socket.error,(ec,es):
                                                if ec!=errno.EINPROGRESS:
                                                        self.processer.ready_output(f)
                                                        try:
                                                                logger.debug("[-] (%s) Ошибка соединения (%s): %s", self.name, f.stock, es)
                                                                sock.shutdown(2)
                                                                sock.close()
                                                        except:
                                                                pass
                                                else:
                                                        writers.append(sock)
                                logger.debug("[+] (%s) Открыли соединения: %.2f сек", self.name, time.time()-t1)
                        try:
                                (r,w,e)=select.select(readers,writers,[],selectblocktime)
                        except:
                                pass
                        # Отослать запросы
                        t3 = time.time()
                        if w:
                                logger.debug("[=] (%s) Отсылаем запросы (цикл: %d)", self.name, cnt)
                                for sock in w:
                                        sent=0
                                        f=threads[sock]
                                        buff=f.c[f.next:f.next+8192]
                                        try:
                                                sent=sock.send(buff)
                                                f.next+=sent
                                                if f.next==f.total:
                                                        writers.remove(sock)
                                                        readers.append(sock)
                                        except socket.error,(ec,es):
                                                writers.remove(sock)
                                                logger.debug("[-] Ошибка соединения (%s): %s" % (f.stock, es))
                                                self.processer.ready_output(f)
                                                try:
                                                        sock.shutdown(2)
                                                        sock.close()
                                                except:
                                                        pass
                                logger.debug("[+] (%s) Запросы отослали: %.2f сек (цикл: %d)", self.name, time.time()-t3, cnt)
                        # Принять данные
                        if r:
                                logger.debug("[=] (%s) Начинаем читать (цикл: %d)", self.name, cnt)
                                for sock in r:
                                        f=threads[sock]
#                                       logger.debug("[D] Читаем слад %s" % f.stock)
                                        try:
                                                buff=sock.recv(8192)
                                                if not buff:
                                                        readers.remove(sock)
                                                        self.processer.ready_output(f)
                                                        try:
                                                                sock.shutdown(2)
                                                                sock.close()
                                                        except:
                                                                pass
                                                else:
                                                        f.contents+=buff
#                                                       logger.debug("[D] Прочитали от склада %s %d байт",f.stock, len(buff))
                                                        if not f.headers:
                                                                try:
                                                                        f.headers,f.response = re.split('\r?\n\r?\n',f.contents,1)
                                                                except:
                                                                        pass
                                                                if f.headers:
                                                                        r = re.search('Content-Length:\s?(\d+)', f.headers, re.I+re.M)
                                                                        f.content_length = int(r.group(1)) if r else 0
                                                        else:
                                                                f.response+=buff
                                                        if f.content_length and f.response and len(f.response) >= f.content_length:
                                                                readers.remove(sock)
                                                                self.processer.ready_output(f)
#                                                               logger.debug("[D] Дочитали склад %s: всего %d байт", f.stock, len(f.contents))
                                                                try:
                                                                        sock.shutdown(2)
                                                                        sock.close()
                                                                except:
                                                                        pass
                                        except socket.error:
                                                readers.remove(sock)
                                                self.processer.ready_output(f)
                                                try:
                                                        sock.shutdown(2)
                                                        sock.close()
                                                except:
                                                        pass
                        # завершить по таймауту
                        if t2 > self.receive_timeout:
                                logger.debug("[!] (%s) Таймаут чтения, закрываем сокеты", self.name)
                                if writers:
                                        for sock in writers:
                                                f=threads[sock]
                                                writers.remove(sock)
                                                self.processer.ready_output(f)
                                                try:
                                                        sock.shutdown(2)
                                                        sock.close()
                                                except:
                                                        pass
                                if readers:
                                        for sock in readers:
                                                f=threads[sock]
                                                readers.remove(sock)
                                                self.processer.ready_output(f)
                                                try:
                                                        sock.shutdown(2)
                                                        sock.close()
                                                except:
                                                        pass
                                logger.debug("[+] (%s) Закрыли сокеты", self.name)
                        if ((len(readers)==0) and (len(writers)==0)):
                                break
                        cnt+=1
                        time.sleep(0.001)
                logger.debug("[=] (%s) Закончили опрос за %2.3f сек %d циклов", self.name, time.time()-t1, cnt)
                # Закроем лишние сокеты
                while sockets:
                        s=sockets.pop(0)
                        del threads[s]
                        s.close()

# конфигурация
myhost='0.0.0.0'
myport=8888
receive_timeout=20
maxhandlers=40

stocks = ["diphost.ru", "exim.org"]

# основной лог
logger=logging.getLogger("main")
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)

try:
	logger.debug("[=] Запуск слушателя")
	# запуск основного треда, выход из него означает завершение программы
	try:
		# открыть сокет
		sockobj=0
		try:
			sockobj=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sockobj.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			sockobj.bind((myhost,myport))
			sockobj.listen(5)
		except:
			logger.critical("[!] Не удалось создать сокет %s", sys.exc_value)
			raise
		handlers=[]
		while 1:
			try:
				# принять соединения
				connection, address = sockobj.accept()
			except:
				# завершить работу по ошибке сокета
				logger.critical("[-] Сокет отвалился %s",sys.exc_value)
				for handler in handlers:
					handler.join()
				break
			# почистить уже отработавшие обработчики
			for handler in handlers:
				if not handler.isAlive():
					handlers.remove(handler)
			# ограничить количество обработчиков
			c=handlers.__len__()
			logger.info("[+] Server connected by %s (%d)", address,c)
			if (c>maxhandlers):
				try:
					connection.sendall("HTTP/1.1 503 Service unavailable\r\n\r\n")
					logger.info("Server BUSY %d",c)
				finally:
					connection.close()
				continue
			try:
				# Запустить обработчик
				p=HandleClient(connection,stocks,receive_timeout)
				p.start()
				handlers.append(p)
			except:
				connection.close()
				logger.error("[!] Не запустить обработчик %s",sys.exc_value)
	except KeyboardInterrupt:
		os._exit()
	logger.debug("[=] Завершение слушателя")
except:
	logger.critical("[!] Не удалось запуститься: %s", sys.exc_value)

logger.warn("[=] Завершение")
os._exit(0)

