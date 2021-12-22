# sql = "SELECT FROM_BASE64(a.path), a.hash, a.size FROM `naive_file_info` a JOIN (SELECT *, COUNT(*) FROM `naive_file_info` GROUP BY `hash` HAVING COUNT(*) > 1) b ON a.hash = b.hash ORDER BY a.hash"

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
import shutil;

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("--backup", help="backup path for folder compacting");
	args = parser.parse_args()
	root_path = os.path.abspath(args.backup);
	print("Folder compacting at {0}....\n".format(root_path));

	try:
		connection = pymysql.connect(host='127.0.0.1',
			user='zzlee',
			password='tyuiop',
			db='files')
		cursor = connection.cursor();

		cursor.execute("SELECT * FROM `files_dup`");
		rows = cursor.fetchall()

		hash = 0;
		proceeded_files = 0;
		files_moved = 0;
		files_failed = 0;
		for row in rows:
			file_path = base64.b64decode(row[0].encode("utf-8")).decode('utf-8');
			new_hash = row[1];
			msg_file_path = re.sub(r'^(.{32}).*(.{32})$', '\g<1>...\g<2>', file_path);

			if new_hash != hash:
				hash = new_hash;
				msg = "({0}) New {1}...".format(proceeded_files, msg_file_path);
			else:
				msg = "({0}) Compacting {1}...".format(proceeded_files, msg_file_path);
				sys.stdout.write(msg);
				sys.stdout.flush();

				if os.access(file_path, os.R_OK):
					msg += " moved.";
					sys.stdout.write("\r" + msg);
					sys.stdout.flush();

					drive, relative_path = os.path.splitdrive(file_path);
					dst_file_path = os.path.join(root_path, relative_path[1:]);
					folder, file = os.path.split(dst_file_path);
					if not os.path.isdir(folder):
						os.makedirs(folder);

					shutil.move(file_path, dst_file_path);

					files_moved += 1;
				else:
					msg += " failed!";
					sys.stdout.write("\r" + msg);
					sys.stdout.flush();

					files_failed += 1;

			sys.stdout.write("\r" + " " * len(bytes(msg, "utf-8")) + "\r");
			sys.stdout.flush();

			proceeded_files += 1;

		print("Moved {0} files, {1} fail".format(files_moved, files_failed));

		connection.commit();
	finally:
		cursor.close();
		connection.close();

if __name__ == "__main__":
	main();