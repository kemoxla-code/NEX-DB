import os
import warnings
import curses
import random
import time
from utils import Timer
from loaders import discover_files, load_csv, load_xlsx, load_sqlite
from analyzers import run_all
from db_analyzers import run_all_db
from report import create_report
import relationships

warnings.filterwarnings("ignore", category=UserWarning)

logo = """
██╗░░██╗██╗░░░░░░█████╗░                   ██╗░░░░░░█████╗░██╗░░░██╗███╗░░░███╗
╚██╗██╔╝██║░░░░░██╔══██╗                   ██║░░░░░██╔══██╗╚██╗░██╔╝████╗░████║
░╚███╔╝░██║░░░░░███████║                   ██║░░░░░███████║░╚████╔╝░██╔████╔██║
░██╔██╗░██║░░░░░██╔══██║     █████████     ██║░░░░░██╔══██║░░╚██╔╝░░██║╚██╔╝██║
██╔╝╚██╗███████╗██║░░██║                   ███████╗██║░░██║░░░██║░░░██║░╚═╝░██║
╚═╝░░╚═╝╚══════╝╚═╝░░╚═╝                   ╚══════╝╚═╝░░╚═╝░░░╚═╝░░░╚═╝░░░░░╚═╝
"""

def matrix_effect(stdscr):
    curses.curs_set(0)
    stdscr.nodelay(True)
    stdscr.timeout(0)

    curses.start_color()
    curses.use_default_colors()


    curses.init_pair(1, curses.COLOR_GREEN, -1)   
    curses.init_pair(2, curses.COLOR_WHITE, -1)   
    curses.init_pair(3, curses.COLOR_GREEN, -1)   
    curses.init_pair(4, curses.COLOR_WHITE, -1)   

    height, width = stdscr.getmaxyx()
    columns = width
    drops = [random.randint(0, height) for _ in range(columns)]

    charset = "SYNTRIXLA*"

    logo_lines = logo.strip('\n').split('\n')
    logo_height = len(logo_lines)
    logo_width = max(len(line) for line in logo_lines)
    logo_y = height // 2 - logo_height // 2
    logo_x = width // 2 - logo_width // 2

    start_time = time.time()

    while True:
        stdscr.clear()

  
        for idx, line in enumerate(logo_lines):
            y = logo_y + idx
            if 0 <= y < height:
                try:
                    stdscr.addstr(y, logo_x, line[:width - logo_x], curses.color_pair(3))
                except curses.error:
                    pass


        for i in range(columns):
            for offset in range(3): 
                y = drops[i] - offset
                char = random.choice(charset)


                if logo_y <= y < logo_y + logo_height and logo_x <= i < logo_x + logo_width:
                    continue

                if 0 <= y < height:
                    color = curses.color_pair(2) if offset == 0 else curses.color_pair(1)
                    try:
                        stdscr.addstr(y, i, char, color)
                    except curses.error:
                        pass

            drops[i] += random.randint(1, 2)
            if drops[i] >= height + 3:
                drops[i] = 0

        stdscr.refresh()
        time.sleep(0.02)

        if time.time() - start_time > 6:
            break

        try:
            key = stdscr.getch()
            if key == ord('q'):
                return
        except:
            pass

    for _ in range(3):
        stdscr.clear()
        for _ in range(200):
            y = random.randint(0, height - 1)
            x = random.randint(0, width - 1)
            try:
                stdscr.addstr(y, x, random.choice(charset), curses.color_pair(2))
            except curses.error:
                pass
        stdscr.refresh()
        time.sleep(0.1)


    stdscr.clear()
    msg = "SYNTRIX SYSTEM BOOT COMPLETE"
    try:
        stdscr.addstr(height // 2, (width - len(msg)) // 2, msg, curses.color_pair(4))
    except curses.error:
        pass
    stdscr.refresh()
    time.sleep(1)

def init(stdscr):
    matrix_effect(stdscr)

def main():
    curses.wrapper(init)

if __name__ == "__main__":
    main()




def main():

    input_folder = input("NEX-DB ==> Enter the path to the folder containing your data: ").strip()


    output_folder = input("NEX-DB ==> Enter the path to save the report: ").strip()
    os.makedirs(output_folder, exist_ok=True)


    report_name = input("NEX-DB ==> Enter a name for the report file (without extension): ").strip()
    if not report_name:
        report_name = "Report"
    output_path = os.path.join(output_folder, f"{report_name}.xlsx")


    similarity_choice = input("NEX-DB ==> Do you want similarity results? (yes/no): ").strip().lower()


    if similarity_choice == "yes":
        central_input = input(
            "NEX-DB ==> Enter your central key file(s) (comma-separated, including extension): "
        ).strip()
        central_files = [n.strip() for n in central_input.split(",") if n.strip()]
    else:
        central_files = []


    files          = discover_files(input_folder)
    all_issues     = {}
    file_encodings = {}
    file_paths     = {}
    file_dfs       = {}

    with Timer() as t:
        for path in files:
            ext      = os.path.splitext(path)[1].lower()
            basename = os.path.basename(path)
            file_paths[basename] = path

            if ext == ".csv":
                df, enc = load_csv(path)
                file_encodings[basename] = enc
                file_dfs[basename] = df
                dfs = {"(csv)": df}

            elif ext == ".xlsx":
                df = load_xlsx(path)
                file_encodings[basename] = "xlsx"
                file_dfs[basename] = df
                dfs = {"(xlsx)": df}

            elif ext in {".db", ".sqlite3"}:
                file_encodings[basename] = ext.lstrip(".")
                db_issues = run_all_db(path)
                print(f"\n--- DB Issues for {basename} ---")
                for issue in db_issues:
                    print(issue)
                continue

            else:
                continue

            for suffix, df in dfs.items():
                key = f"{basename} {suffix}"
                all_issues[key] = run_all(df)


    time_stats = {
        "start":     t.start,
        "end":       t.end,
        "elapsed_s": t.elapsed
    }


    create_report(all_issues, time_stats, file_encodings, file_paths, output_path)

    if similarity_choice == "yes":
        rels = relationships.compute_relationships(
            file_dfs,
            central_files,
            threshold=0.9
        )
        relationships.add_relationships_to_report(output_path, rels)

    print("")
    logo = """
    ██╗░░██╗██╗░░░░░░█████╗░
    ╚██╗██╔╝██║░░░░░██╔══██╗
    ░╚███╔╝░██║░░░░░███████║
    ░██╔██╗░██║░░░░░██╔══██║
    ██╔╝╚██╗███████╗██║░░██║
    ╚═╝░░╚═╝╚══════╝╚═╝░░╚═╝
    """
    print(logo)
    print(f"\nNEX-DB ==> Report saved at: {output_path}\n")

if __name__ == "__main__":
    main()
