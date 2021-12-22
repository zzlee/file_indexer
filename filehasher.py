import sys;
import os;
import os.path;
import argparse
import hashlib;
import pymysql;
import base64;
import datetime;
import time;
import re;

def gen_file_hash(path, chunk_size=8192):
	with open(path, "rb") as f:
		file_hash = hashlib.md5()
		while True:
			chunk = f.read(chunk_size);
			if not chunk:
				break;
			file_hash.update(chunk);

	return file_hash;

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("--path", help="path for folder to hashing");
	args = parser.parse_args()
	root_path = os.path.abspath(args.path);
	print("Folder hashing at {0}....\n".format(root_path));

	try:
		connection = pymysql.connect(host='127.0.0.1',
			user='zzlee',
			password='tyuiop',
			db='files')
		cursor = connection.cursor();

		proceeded_files = 0;
		for root, dir, files in os.walk(root_path):
			for file in files:
				file_path = os.path.join(root, file);
				if not os.access(file_path, os.R_OK):
					continue;

				msg = "({0}) Hasing \"{1}\" ...".format(proceeded_files,
					re.sub(r'^(.{32}).*(.{32})$', '\g<1>...\g<2>', file_path));
				sys.stdout.write(msg);
				sys.stdout.flush();
				hash = gen_file_hash(file_path);
				sql = "SELECT `path`, `hash`, `size` FROM `naive_file_info` WHERE `path` = %s AND `hash` = %s";
				cursor.execute(sql, (base64.b64encode(file_path.encode("utf-8")), hash.hexdigest()));
				result = cursor.fetchone();
				if result != None:
					msg += " exists!";
					sys.stdout.write("\r" + msg);
					sys.stdout.flush();
				else:
					msg += " recording...";
					sys.stdout.write("\r" + msg);
					sys.stdout.flush();

					sql = "INSERT INTO `naive_file_info`(`path`, `hash`, `size`, `create_datetime`) VALUES (%s,%s,%s,%s)";
					cursor.execute(sql, (base64.b64encode(file_path.encode("utf-8")), hash.hexdigest(), os.path.getsize(file_path), datetime.datetime.now()));
				connection.commit()

				# time.sleep(1);
				sys.stdout.write("\r" + " " * len(bytes(msg, "utf-8")) + "\r");
				sys.stdout.flush();
				# time.sleep(1);

				proceeded_files += 1;
	finally:
		cursor.close();
		connection.close();

if __name__ == "__main__":
	main();