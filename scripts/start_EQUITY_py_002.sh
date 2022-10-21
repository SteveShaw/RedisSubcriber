#!/bin/bash
screen -dmS EQRE_SUFFIX-PY-02_FILES-PY-02 $HOME/etc/bin/LevelReceiver EQUITY EQUITY-SUFFIX-PY-02 EQUITY-FILES-PY-02 FEATHER
screen -dmS EQPYLIVE-FILES-PY-02 python3 $HOME/etc/bin/equity_option_to_db.py -k EQUITY-FILES-PY-02 -l
screen -dmS EQPYHIST-FILES-PY-02 python3 $HOME/etc/bin/equity_option_to_db.py -k EQUITY-FILES-PY-02-HIST -b
