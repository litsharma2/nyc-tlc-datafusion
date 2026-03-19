import sys
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

def main():
    if len(sys.argv) != 3:
        print("Usage: make_screenshot.py <input.txt> <output.png>")
        sys.exit(2)

    in_path = Path(sys.argv[1])
    out_path = Path(sys.argv[2])
    text = in_path.read_text(errors="replace")

    lines = text.splitlines()
    max_lines = 250
    if len(lines) > max_lines:
        lines = lines[:max_lines] + ["", f"... (truncated to {max_lines} lines for screenshot)"]

    font = None
    for name in ["DejaVuSansMono.ttf", "Consolas.ttf", "Courier New.ttf"]:
        try:
            font = ImageFont.truetype(name, 16)
            break
        except Exception:
            pass
    if font is None:
        font = ImageFont.load_default()

    padding = 20
    line_height = int(font.getbbox("A")[3] * 1.4)
    width = 1200
    height = padding * 2 + line_height * len(lines)

    img = Image.new("RGB", (width, height), (20, 20, 20))
    draw = ImageDraw.Draw(img)

    y = padding
    for line in lines:
        while len(line) > 140:
            draw.text((padding, y), line[:140], font=font, fill=(230, 230, 230))
            line = line[140:]
            y += line_height
        draw.text((padding, y), line, font=font, fill=(230, 230, 230))
        y += line_height

    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path)
    print(f"Wrote {out_path}")

if __name__ == "__main__":
    main()