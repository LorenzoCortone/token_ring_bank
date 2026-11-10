@echo off
cd /d "%~dp0"
where py >nul 2>nul
if %errorlevel%==0 (
    set PY_CMD=py
) else (
    set PY_CMD=python
)

echo Apertura dei 4 terminali in pausa (pronti per la registrazione)...
start "ATM 4 (Porta 5004)" cmd /k "mode con: cols=75 lines=18 & %PY_CMD% atm_node.py 4 --tx PRELIEVO 500"
start "ATM 3 (Porta 5003)" cmd /k "mode con: cols=75 lines=18 & %PY_CMD% atm_node.py 3 --tx DEPOSITO 100"
start "ATM 2 (Porta 5002)" cmd /k "mode con: cols=75 lines=18 & %PY_CMD% atm_node.py 2 --tx PRELIEVO 200"
start "ATM 1 (Porta 5001)" cmd /k "mode con: cols=75 lines=18 & %PY_CMD% atm_node.py 1"
exit