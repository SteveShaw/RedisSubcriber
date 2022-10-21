#!/bin/bash
screen -dmS CRYPTO_RECEIVER_SUFFIX-PY-01_FILES-PY-01 $HOME/etc/bin/LevelReceiver CRYPTO CRYPTO-SUFFIX-PY-01 CRYPTO-FILES-PY-01 FEATHER
screen -dmS CRYPTO_PYLIVE-FILES-PY-01 python3 $HOME/etc/bin/crypto_equity_to_db.py -k CRYPTO-FILES-PY-01 -l
screen -dmS CRYPTO_PYHIST-FILES-PY-01 python3 $HOME/etc/bin/crypto_equity_to_db.py -k CRYPTO-FILES-PY-01-HIST -b
