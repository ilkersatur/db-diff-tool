# DB Diff Tool

Bu proje, iki farkli veritabani baglantisini (`dev` ve `test`) karsilastirmak icin hazirlandi.

## Ozellikler

- Iki connection string ile baglanir.
- Her iki taraftaki tablolari listeler.
- Secilen tablo icin kolon ve tip bilgilerini karsilastirir.
- Fark durumlarini renkle gosterir:
  - Sadece `dev` tarafinda olan: yesil
  - Sadece `test` tarafinda olan: kirmizi
  - Ayni olan: gri
  - Tip farki olan: sari
- "Ayni olanlari gizle" secenegi vardir.
- Opsiyonel veri karsilastirmasi:
  - Anahtar kolon secerek satir bazli fark bulma
  - Sadece dev/test'te olan satirlari listeleme
  - Her ikisinde olup degeri farkli olan satirlari isaretleme

## Ilk Kurulum (Bir kere)

```bash
cd C:\Users\ilkers\Desktop\db-diff-tool
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Sonraki Calistirmalar (Her seferinde)

```bash
cd C:\Users\ilkers\Desktop\db-diff-tool
.\.venv\Scripts\Activate.ps1
streamlit run app.py
```

## Kapatma

- Uygulamayi durdurmak icin terminalde `Ctrl + C` yapin.
- Sonra terminali kapatabilirsiniz.

## Notlar

- Connection string, SQLAlchemy formatinda olmalidir.
- Veri karsilastirmasinda anahtar kolon secimi zorunludur.
- Cok buyuk tablolarda performans icin satir limiti kullanin.
- Eger `Activate.ps1` calismazsa bir kere su komutu verin:
  `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`
