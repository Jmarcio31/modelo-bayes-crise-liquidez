ARQUIVOS DESTA ETAPA

1) backend/app/data_builder.py
   - coleta TIC, repo stress, FRA proxy, RRP e USD stress
   - gera backend/data/raw/data_feed.csv

2) backend/app/feed_loader.py
   - permite ao backend principal ler o data_feed.csv com metadados

3) backend/app/main.py
   - passa a consumir automaticamente o data_feed.csv
   - injeta data_feed_meta no latest.json

4) backend/app/config.py
   - mantém os caminhos atualizados para o site na raiz (data/latest.json e data/history.json)

5) backend/run_weekly.py
   - roda primeiro o data_builder e depois o pipeline principal

6) .github/workflows/weekly.yml
   - executa data_builder.py antes do backend principal
   - versiona data/latest.json, data/history.json e backend/data/raw/data_feed.csv

COMO APLICAR NO SEU PROJETO

Substitua os arquivos correspondentes no seu repositório local.

Depois rode localmente:

1) python -m backend.app.data_builder
2) python -m backend.app.main

Verifique:
- backend/data/raw/data_feed.csv
- data/latest.json
- data/history.json

Se estiver tudo certo:
- git add .
- git commit -m "Implementa feed externo automatizado"
- git pull --rebase origin main
- git push

OBSERVAÇÕES IMPORTANTES

- O TIC usa scraping da tabela oficial slt_table3 do Tesouro.
- O repo_stress_score usa a API pública de secured rates do NY Fed.
- O FRA-OIS continua sendo um proxy calculado, mas passa a ser automático.
- Se uma fonte falhar, o data_builder tenta reaproveitar o último valor salvo no data_feed.csv com quality_flag=stale_fallback.
