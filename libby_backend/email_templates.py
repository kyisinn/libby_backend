from typing import List, Dict
import html, os

FRONTEND_BASE = os.getenv("FRONTEND_BASE", "https://example.com")

def _truncate_two_lines(text: str, max_chars: int = 68) -> str:
    if not text: return ""
    t = " ".join(text.split())
    return (t[: max_chars - 1] + "…") if len(t) > max_chars else t

def au_bibliophiles_recs_html(books: List[Dict], explore_url: str) -> str:
    books = (books or [])[:4]
    while len(books) < 4:
        books.append({})

    def card(b):
        if not b or not b.get("title"):
            return f"""
            <td width="180" align="left" valign="top" style="padding:0 12px 0 0;">
              <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
                <tr><td height="260" style="font-size:0;line-height:0;">&nbsp;</td></tr>
                <tr><td style="font-size:0;line-height:0;">&nbsp;</td></tr>
              </table>
            </td>
            """
        title = html.escape(b.get("title","Untitled"))
        author = html.escape(b.get("author","Unknown Author"))
        cover = (b.get("cover_image_url") or "").replace("http://","https://").strip()
        detail_url = b.get("detail_url") or f"{FRONTEND_BASE}/book/{html.escape(str(b.get('id','')))}"
        
        # Use title and author for the description instead of blurb
        description = f"{title} by {author}"
        blurb = html.escape(_truncate_two_lines(description, 68))
        
        # Use actual cover image or fallback to placeholder
        image_url = cover if cover else "https://via.placeholder.com/180x260/e8edf9/2042b2?text=No+Cover"
        
        return f"""
        <td width="180" align="left" valign="top" style="padding:0 12px 0 0;">
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
            <tr>
              <td style="border-radius:10px;overflow:hidden;">
                <a href="{html.escape(detail_url)}" style="text-decoration:none;border:0;display:block;">
                  <img src="{html.escape(image_url)}" width="180" height="260" alt="{html.escape(title)}" style="display:block;border:0;outline:none;text-decoration:none;border-radius:10px;box-shadow:0 4px 14px rgba(0,0,0,0.12);object-fit:cover;">
                </a>
              </td>
            </tr>
            <tr><td height="10" style="line-height:10px;font-size:10px;">&nbsp;</td></tr>
            <tr>
              <td style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;color:#111;">
                <a href="{html.escape(detail_url)}" style="text-decoration:none;color:#111;">
                  <div style="font-size:14px;line-height:20px;font-weight:600;margin-bottom:4px;">
                    {html.escape(title)}
                  </div>
                  <div style="font-size:13px;line-height:18px;color:#666;">
                    by {html.escape(author)}
                  </div>
                </a>
              </td>
            </tr>
          </table>
        </td>
        """

    cards_row = "".join(card(b) for b in books)

    return f"""\
<!doctype html>
<html>
  <head>
    <meta name="x-apple-disable-message-reformatting">
    <meta name="format-detection" content="telephone=no,date=no,address=no,email=no,url=no">
    <meta name="color-scheme" content="light only">
    <meta name="supported-color-schemes" content="light only">
  </head>
  <body style="margin:0;padding:0;background:#ffffff;">
    <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="background:#ffffff;">
      <tr><td align="center">
        <table role="presentation" width="640" border="0" cellspacing="0" cellpadding="0" style="width:640px;max-width:100%;">
          <tr>
            <td style="padding:28px 24px 8px 24px;font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;color:#111;">
              <div style="font-size:20px;font-weight:700;">AU Bibliophiles</div>
            </td>
          </tr>
          <tr>
            <td style="padding:4px 24px 18px 24px;font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;color:#111;">
              <div style="font-size:22px;line-height:28px;font-weight:700;">
                Recommended Reads Book Suggestions
              </div>
            </td>
          </tr>
          <tr>
            <td style="padding:0 24px 8px 24px;">
              <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
                <tr>{cards_row}</tr>
              </table>
            </td>
          </tr>
          <tr>
            <td align="right" style="padding:6px 24px 28px 24px;">
              <table role="presentation" cellspacing="0" cellpadding="0" border="0">
                <tr>
                  <td bgcolor="#e8edf9" style="border-radius:8px;">
                    <a href="{explore_url}"
                       style="display:inline-block;padding:10px 14px;font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;
                              font-size:14px;font-weight:600;color:#2042b2;text-decoration:none;">
                      Explore more
                    </a>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
        </table>
      </td></tr>
    </table>
  </body>
</html>
"""