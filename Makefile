PYTHON ?= python3
PORT ?= 4000

.PHONY: check test server client

check:
	$(PYTHON) -m py_compile protocol.py servidor.py cliente.py

test: check
	$(PYTHON) -m unittest discover -s tests -v

server:
	$(PYTHON) servidor.py $(PORT)

client:
	$(PYTHON) cliente.py $(PORT)
