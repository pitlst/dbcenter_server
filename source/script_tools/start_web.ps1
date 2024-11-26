conda activate nuitka_make
Set-Location ../web
while ($true) {
    python -m streamlit run home.py
}