"""
PROGETTO 1: SISTEMA DI TRANSAZIONI BANCARIE DISTRIBUITE CON MUTUA ESCLUSIONE A TOKEN RING
Autore: [Lorenzo Cortone] | Matricola: [0322600018]
"""

import socket
import threading
import json
import time
import sys
import os
import queue
from datetime import datetime

# TOPOLOGIA LOGICA AD ANELLO E ASSENZA DI MEMORIA CONDIVISA
# I 4 nodi ATM sono processi indipendenti senza memoria condivisa.
# L'anello logico (ATM1 -> ATM2 -> ATM3 -> ATM4 -> ATM1) e' realizzato su localhost:
# ogni nodo conosce esclusivamente la propria porta di ascolto (my_port) e la porta
# del proprio successore diretto (next_port), garantendo un instradamento unidirezionale

HOST = '127.0.0.1'

NODE_CONFIG = {
    1: {"my_port": 5001, "next_id": 2, "next_port": 5002},
    2: {"my_port": 5002, "next_id": 3, "next_port": 5003},
    3: {"my_port": 5003, "next_id": 4, "next_port": 5004},
    4: {"my_port": 5004, "next_id": 1, "next_port": 5001},
}

INITIAL_BALANCE = 1000.0
TOKEN_HOLD_TIME = 2.0  # Pausa inserita per avere il tempo di leggere il passaggio nel video
LEDGER_FILE = "shared_bank_account.json"


class ATMNode:
    def __init__(self, node_id: int):
        if node_id not in NODE_CONFIG:
            raise ValueError("L'ID del nodo deve essere compreso tra 1 e 4.")

        self.node_id = node_id
        self.name = f"ATM{node_id}"
        self.my_port = NODE_CONFIG[node_id]["my_port"]
        self.next_id = NODE_CONFIG[node_id]["next_id"]
        self.next_name = f"ATM{self.next_id}"
        self.next_port = NODE_CONFIG[node_id]["next_port"]

        self.transaction_queue = queue.Queue()
        self.has_token = False
        self.running = True
        # IMPOSTATO A FALSE: Nasconde i passaggi a vuoto del token per evitare lo scrolling infinito
        self.show_token_logs = False 

    def log(self, event_type: str, message: str, is_token_movement: bool = False):
        if is_token_movement and not self.show_token_logs:
            return
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"[{timestamp}] [{self.name}] [{event_type:<18}] {message}", flush=True)

    def _init_ledger(self, initial_balance: float):
        data = {"balance": initial_balance, "last_modified_by": self.name}
        with open(LEDGER_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

    def _read_balance(self, token_data: dict) -> float:
        if os.path.exists(LEDGER_FILE):
            with open(LEDGER_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return float(data["balance"])
        return float(token_data["balance"])

    def _write_balance(self, new_balance: float, token_data: dict):
        token_data["balance"] = new_balance
        data = {
            "balance": new_balance,
            "last_modified_by": self.name,
            "timestamp": datetime.now().isoformat()
        }
        with open(LEDGER_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

    
    # LOGICA DELLA SEZIONE CRITICA (MUTUA ESCLUSIONE E ATOMICITA')
    # Questo metodo implementa la Sezione Critica del sistema distribuito
    # PROPRIETA' DI SAFETY: Viene invocato esclusivamente quando il nodo detiene
    # il token (self.has_token == True). Poiche' nel sistema esiste un unico token,
    # e' impossibile che due ATM eseguano una transazione in modo concorrente
    # se la coda locale delle transazioni e' vuota (NO-OP), il nodo rilascia
    # immediatamente la sezione critica per non rallentare l'anello.

    def execute_critical_section(self, token_data: dict) -> dict:
        if self.transaction_queue.empty():
            self.log("NO-OP", f"Nessuna transazione. Saldo: {token_data['balance']:.2f}", is_token_movement=True)
            return token_data

        tx_type, amount = self.transaction_queue.get()
        self.log("CS-ENTER", f"*** INIZIO TRANSAZIONE ({tx_type.upper()} di {amount:.2f}) ***")

        # PASSO 1 - LETTURA DEL SALDO CORRENTE:
        # Acquisisce lo stato aggiornato della risorsa condivisa senza race condition

        current_balance = self._read_balance(token_data)
        self.log("TX-STEP-1 (READ)", f"Saldo corrente letto: {current_balance:.2f}")
        time.sleep(0.5)

        # PASSO 2 - VALIDAZIONE DELL'OPERAZIONE:
        # Verifica la consistenza dell'importo (> 0) e, in caso di prelievo,
        # controlla che il saldo attuale copra la richiesta (current_balance >= amount)

        is_valid = False
        if tx_type == "DEPOSITO":
            if amount > 0:
                is_valid = True
                self.log("TX-STEP-2 (VALID)", "Validazione OK: Deposito valido.")
            else:
                self.log("TX-STEP-2 (VALID)", "ERRORE: Importo non valido.")
        elif tx_type == "PRELIEVO":
            if amount <= 0:
                self.log("TX-STEP-2 (VALID)", "ERRORE: Importo non valido.")
            elif current_balance >= amount:
                is_valid = True
                self.log("TX-STEP-2 (VALID)", f"Validazione OK: Fondi sufficienti ({current_balance:.2f} >= {amount:.2f}).")
            else:
                self.log("TX-STEP-2 (VALID)", f"RIFIUTATA: Fondi insufficienti (Richiesto: {amount:.2f}, Saldo: {current_balance:.2f}).")
        
        # PASSO 3 E PASSO 4 - AGGIORNAMENTO E SCRITTURA DEL NUOVO VALORE:
        # Calcola il nuovo saldo e propaga la modifica sia sul registro persistente
        # sia all'interno del payload del token che verra' inviato al successore
        
        if is_valid:
            new_balance = (current_balance + amount) if tx_type == "DEPOSITO" else (current_balance - amount)
            self.log("TX-STEP-3 (UPDATE)", f"Aggiornamento saldo: {current_balance:.2f} -> {new_balance:.2f}")
            self._write_balance(new_balance, token_data)
            self.log("TX-STEP-4 (WRITE)", f"Nuovo valore scritto: {new_balance:.2f}")
            token_data["tx_count"] += 1
        else:
            new_balance = current_balance

        # PASSO 5 - REGISTRAZIONE (LOG) DELL'OPERAZIONE E USCITA DALLA SEZIONE CRITICA:
        # Registra il completamento atomico della transazione prima di cedere il token

        self.log("CS-EXIT", f"*** FINE TRANSAZIONE | SALDO AGGIORNATO: {new_balance:.2f} ***")
        self.transaction_queue.task_done()
        return token_data

    # SCAMBIO DI MESSAGGI (INOLTRO DEL TOKEN AL NODO SUCCESSORE)
    # Il coordinamento avviene interamente tramite scambio di messaggi su socket TCP
    # il token viene serializzato in formato JSON (trasportando il saldo aggiornato e
    # il numero di sequenza incrementato) e trasmesso con connessione point-to-point
    # esclusivamente al nodo successore (self.next_port)
    # il ciclo while con gestione delle eccezioni garantisce tolleranza ai ritardi di
    # avvio dei nodi finche' l'anello non e' completamente online

    def forward_token(self, token_data: dict):
        token_data["sender"] = self.name
        token_data["sequence_num"] += 1
        payload = json.dumps(token_data).encode('utf-8')

        while self.running:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(2.0)
                    s.connect((HOST, self.next_port))
                    s.sendall(payload)
                self.has_token = False
                self.log("TOKEN-SEND", f"Token inoltrato a {self.next_name} | Saldo: {token_data['balance']:.2f}", is_token_movement=True)
                break
            except (ConnectionRefusedError, socket.timeout, OSError):
                time.sleep(1.0)

    # GESTIONE DEL TOKEN (ACQUISIZIONE, POSSESSO E RILASCIO)
    # Gestisce il ciclo di vita del token all'interno del singolo nodo:
    # 1. Acquisisce il diritto di accesso esclusivo (self.has_token = True)
    # 2. Verifica ed esegue un'eventuale transazione pendente entrando in Sezione Critica
    # 3. Cede il permesso inoltrando il token al successore (evitando starvation
    # degli altri nodi grazie al rilascio garantito dopo al massimo 1 operazione)

    def handle_incoming_token(self, token_data: dict):
        self.has_token = True
        sender = token_data.get("sender", "INIT")
        self.log("TOKEN-RECV", f"Token ricevuto da {sender} (Seq #{token_data['sequence_num']}) | Saldo: {token_data['balance']:.2f}", is_token_movement=True)

        time.sleep(TOKEN_HOLD_TIME)
        updated_token = self.execute_critical_section(token_data)
        self.forward_token(updated_token)

    # RICEZIONE DEI MESSAGGI DALL'ANELLO (SERVER TCP ASINCRONO)
    # Thread in ascolto continuo sulla porta privata del nodo (self.my_port)
    # Riceve il flusso di byte TCP dal nodo predecessore, ricostruisce il messaggio
    # JSON del token e avvia la procedura di gestione (handle_incoming_token)

    def server_listener_loop(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_sock.bind((HOST, self.my_port))
            server_sock.listen(5)
            server_sock.settimeout(1.0)

            self.log("SYSTEM-START", f"Online su {HOST}:{self.my_port} -> Successore: {self.next_name}")

            while self.running:
                try:
                    conn, _ = server_sock.accept()
                    with conn:
                        raw_data = conn.recv(4096)
                        if raw_data:
                            token_data = json.loads(raw_data.decode('utf-8'))
                            self.handle_incoming_token(token_data)
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:
                        pass

    def start(self):
        
        print(f"  NODO {self.name} (Porta {self.my_port}) - PRONTO")
        print("_" * 65)
        
        # PAUSA MANUALE: Il terminale si ferma qui e aspetta che tu prema INVIO 
        # Inserito perche' non riuscivo a catturare in video mentre sistemavo le finestre dei terminali
        input(f"[{self.name}] Premi INVIO in questo terminale per metterlo online... ")
        print(f"[{self.name}] Avviato! In attesa...")

        listener_thread = threading.Thread(target=self.server_listener_loop, daemon=True)
        listener_thread.start()

        # GENERAZIONE DEL TOKEN UNICO INIZIALE (BOOTSTRAP DELL'ANELLO):
        # Come richiesto dalla specifica (Sezione 6 e 7), nel sistema esiste esattamente
        # un solo token, posseduto inizialmente da ATM1 con saldo iniziale pari a 1000.00

        if self.node_id == 1:
            self._init_ledger(INITIAL_BALANCE)
            initial_token = {
                "token_id": "BANK_MUTEX_TOKEN_01",
                "balance": INITIAL_BALANCE,
                "sequence_num": 0,
                "tx_count": 0,
                "sender": "BOOTSTRAP"
            }
            self.log("BOOTSTRAP", f"Generazione token iniziale. Saldo: {INITIAL_BALANCE:.2f}")
            threading.Thread(target=self.handle_incoming_token, args=(initial_token,), daemon=True).start()

        try:
            while self.running:
                cmd_line = input().strip()
                if not cmd_line:
                    continue
                parts = cmd_line.split()
                cmd = parts[0].lower()

                if cmd in ['d', 'deposito'] and len(parts) == 2:
                    try:
                        amount = float(parts[1])
                        self.transaction_queue.put(("DEPOSITO", amount))
                        self.log("QUEUE-ADD", f"Accodato DEPOSITO di {amount:.2f}")
                    except ValueError:
                        print("[!] Importo non valido. Usa: d 100")

                elif cmd in ['p', 'prelievo'] and len(parts) == 2:
                    try:
                        amount = float(parts[1])
                        self.transaction_queue.put(("PRELIEVO", amount))
                        self.log("QUEUE-ADD", f"Accodato PRELIEVO di {amount:.2f}")
                    except ValueError:
                        print("[!] Importo non valido. Usa: p 200")

                elif cmd in ['l', 'log']:
                    self.show_token_logs = not self.show_token_logs
                    print(f"[*] Log passaggi a vuoto: {'ATTIVI' if self.show_token_logs else 'NASCOSTI'}")

                elif cmd in ['q', 'esci']:
                    self.running = False
                    break
        except (KeyboardInterrupt, EOFError):
            self.running = False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python atm_node.py <ID_NODO> [--tx <TIPO> <IMPORTO>]")
        sys.exit(1)

    node_num = int(sys.argv[1])
    atm = ATMNode(node_num)

    if len(sys.argv) == 5 and sys.argv[2] == "--tx":
        tx_type_arg = sys.argv[3].upper()
        tx_amount_arg = float(sys.argv[4])
        if tx_type_arg in ["DEPOSITO", "PRELIEVO"]:
            atm.transaction_queue.put((tx_type_arg, tx_amount_arg))

    atm.start()