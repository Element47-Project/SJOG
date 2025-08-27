import os


def save_dropped_files(files, target_dir):
    success, failed = [], []
    for src in files:
        if os.path.isfile(src):
            dst = os.path.join(target_dir, os.path.basename(src))
            try:
                with open(src, 'rb') as fsrc, open(dst, 'wb') as fdst:
                    fdst.write(fsrc.read())
                success.append(src)
            except Exception as e:
                failed.append((src, str(e)))
    return success, failed
