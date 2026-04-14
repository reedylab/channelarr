"""Channel logo generation from URL sources.

Handles downloading logos (PNG, JPEG, SVG), extracting dominant colors,
and compositing matchup cards for two-team events.

Logo rules:
  - 1 URL:  download, convert to PNG, resize to 400x400
  - 2 URLs: diagonal split matchup card with team colors
  - 3+ URLs: use the first one only
"""

import io
import logging
import os
from colorsys import rgb_to_hsv

import requests
from PIL import Image, ImageDraw

logger = logging.getLogger(__name__)

CARD_SIZE = 400
LOGO_SIZE = 140
LOGO_PADDING = 30
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def _download_image(url: str) -> Image.Image | None:
    """Download an image URL and return a Pillow Image. Handles SVG via cairosvg."""
    try:
        resp = requests.get(url, headers={"User-Agent": UA}, timeout=15)
        resp.raise_for_status()
        content = resp.content
        content_type = resp.headers.get("Content-Type", "")

        # SVG detection — by content-type or file extension or content sniff
        is_svg = (
            "svg" in content_type.lower()
            or url.lower().endswith(".svg")
            or content[:200].strip().startswith((b"<svg", b"<?xml"))
        )

        if is_svg:
            try:
                import cairosvg
                png_data = cairosvg.svg2png(
                    bytestring=content,
                    output_width=CARD_SIZE,
                    output_height=CARD_SIZE,
                )
                return Image.open(io.BytesIO(png_data)).convert("RGBA")
            except Exception as e:
                logger.warning("[LOGO] cairosvg failed for %s: %s", url, e)
                return None

        return Image.open(io.BytesIO(content)).convert("RGBA")
    except Exception as e:
        logger.warning("[LOGO] Download failed for %s: %s", url, e)
        return None


def _dominant_color(img: Image.Image) -> tuple[int, int, int]:
    """Extract the dominant non-gray, non-transparent color from an image.

    Returns an (R, G, B) tuple. Falls back to a neutral dark blue-gray
    if no clear dominant color is found.
    """
    fallback = (30, 40, 55)

    try:
        # Shrink for speed
        small = img.copy()
        small.thumbnail((80, 80), Image.LANCZOS)

        pixels = []
        for pixel in small.getdata():
            r, g, b = pixel[0], pixel[1], pixel[2]
            a = pixel[3] if len(pixel) > 3 else 255

            # Skip transparent, near-white, near-black
            if a < 128:
                continue
            if r > 230 and g > 230 and b > 230:
                continue
            if r < 25 and g < 25 and b < 25:
                continue

            # Skip grays (low saturation)
            h, s, v = rgb_to_hsv(r / 255, g / 255, b / 255)
            if s < 0.15:
                continue

            pixels.append((r, g, b))

        if not pixels:
            return fallback

        # Quantize to find dominant color
        color_img = Image.new("RGB", (len(pixels), 1))
        color_img.putdata(pixels)
        quantized = color_img.quantize(colors=3, method=Image.Quantize.FASTOCTREE)
        palette = quantized.getpalette()
        if palette:
            # First palette entry is the most common
            return (palette[0], palette[1], palette[2])

        return fallback
    except Exception as e:
        logger.debug("[LOGO] Color extraction failed: %s", e)
        return fallback


def _darken(color: tuple[int, int, int], factor: float = 0.7) -> tuple[int, int, int]:
    """Darken a color for better contrast with white/light logos."""
    return (int(color[0] * factor), int(color[1] * factor), int(color[2] * factor))


def _make_single_logo(img: Image.Image) -> Image.Image:
    """Resize a single logo to a square card with transparent background."""
    canvas = Image.new("RGBA", (CARD_SIZE, CARD_SIZE), (0, 0, 0, 0))
    # Fit the logo within the card with padding
    img.thumbnail((CARD_SIZE - 40, CARD_SIZE - 40), Image.LANCZOS)
    x = (CARD_SIZE - img.width) // 2
    y = (CARD_SIZE - img.height) // 2
    canvas.paste(img, (x, y), img)
    return canvas


def _make_matchup_card(img_a: Image.Image, img_b: Image.Image) -> Image.Image:
    """Generate a diagonal split matchup card.

    Team A's dominant color fills the top-left triangle, team B's fills
    the bottom-right. Logos are placed in opposite corners.
    """
    color_a = _darken(_dominant_color(img_a))
    color_b = _darken(_dominant_color(img_b))

    # Create canvas with team B color as base
    canvas = Image.new("RGB", (CARD_SIZE, CARD_SIZE), color_b)
    draw = ImageDraw.Draw(canvas)

    # Draw team A color as top-left triangle (diagonal from top-right to bottom-left)
    draw.polygon(
        [(0, 0), (CARD_SIZE, 0), (0, CARD_SIZE)],
        fill=color_a,
    )

    # Subtle divider line along the diagonal
    draw.line(
        [(CARD_SIZE, 0), (0, CARD_SIZE)],
        fill=(255, 255, 255, 80),
        width=2,
    )

    # Convert to RGBA for logo compositing
    canvas = canvas.convert("RGBA")

    # Place team A logo in top-left
    logo_a = img_a.copy()
    logo_a.thumbnail((LOGO_SIZE, LOGO_SIZE), Image.LANCZOS)
    canvas.paste(logo_a, (LOGO_PADDING, LOGO_PADDING), logo_a)

    # Place team B logo in bottom-right
    logo_b = img_b.copy()
    logo_b.thumbnail((LOGO_SIZE, LOGO_SIZE), Image.LANCZOS)
    bx = CARD_SIZE - logo_b.width - LOGO_PADDING
    by = CARD_SIZE - logo_b.height - LOGO_PADDING
    canvas.paste(logo_b, (bx, by), logo_b)

    return canvas


def generate_channel_logo(channel_id: str, logo_urls: list[str],
                          logo_dir: str = "/app/data/logos") -> bool:
    """Download logo URL(s) and generate a channel logo PNG.

    Returns True if a logo was successfully generated and saved.
    """
    if not logo_urls:
        return False

    # Limit to first 2
    urls = logo_urls[:2]

    images = []
    for url in urls:
        img = _download_image(url)
        if img:
            images.append(img)

    if not images:
        logger.warning("[LOGO] No images downloaded for channel %s", channel_id)
        return False

    if len(images) == 1:
        result = _make_single_logo(images[0])
    else:
        result = _make_matchup_card(images[0], images[1])

    # Save as PNG
    os.makedirs(logo_dir, exist_ok=True)
    out_path = os.path.join(logo_dir, f"{channel_id}.png")
    result.convert("RGB").save(out_path, "PNG")
    logger.info("[LOGO] Generated logo for channel %s (%d source(s))",
                channel_id, len(images))
    return True
