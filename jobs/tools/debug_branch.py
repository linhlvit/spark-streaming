import oracledb

conn = oracledb.connect(user='LPB_POC', password='LPB_POC', dsn='192.168.26.180:1521/dbpdb')
cur = conn.cursor()

# 1. Đếm T24_BRANCH
cur.execute('SELECT COUNT(*) FROM LPB_POC.T24_BRANCH')
print('T24_BRANCH count:', cur.fetchone()[0])

# 2. Sample 5 row đầu
cur.execute('SELECT BRANCH_CODE, BRANCH_NAME FROM LPB_POC.T24_BRANCH WHERE ROWNUM <= 5')
print('Sample T24_BRANCH:')
for r in cur.fetchall():
    print(f'  code={repr(r[0])}  name={repr(r[1])}')

# 3. Lookup BR0201 cụ thể
cur.execute("SELECT BRANCH_CODE, BRANCH_NAME FROM LPB_POC.T24_BRANCH WHERE BRANCH_CODE = 'BR0201'")
row = cur.fetchone()
print('BR0201 exact lookup:', row)

# 4. Kiểm tra length và dump byte để phát hiện whitespace/encoding ẩn
cur.execute("SELECT BRANCH_CODE, LENGTH(BRANCH_CODE), DUMP(BRANCH_CODE) FROM LPB_POC.T24_BRANCH WHERE ROWNUM <= 3")
print('Length/Dump:')
for r in cur.fetchall():
    print(f'  {repr(r[0])}  len={r[1]}  dump={r[2]}')

# 5. Thử load branch_map như trong code
branch_map = {
    r[0]: {"BRANCH_NAME": r[1], "REGION_CODE": r[2], "REGION_NAME": r[3]}
    for r in cur.execute('SELECT BRANCH_CODE, BRANCH_NAME, REGION_CODE, REGION_NAME FROM LPB_POC.T24_BRANCH').fetchall()
}
print(f'\nbranch_map size: {len(branch_map)}')
print('BR0201 in map:', branch_map.get('BR0201'))
print('Sample keys:', list(branch_map.keys())[:5])

conn.close()
