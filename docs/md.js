/* Minimal, self-contained Markdown renderer for the Valdr site (no CDN / no
 * framework). Supports: # headings, nested "- " bullets (2-space indent = one
 * level), "- [ ]"/"- [x]" tasks, GitHub-style | pipe | tables | (with
 * :--/--:/:-: alignment), **bold**, `code`, and [links](url).
 *
 * Used by roadmap.html and coverage.html, each of which loads a .md file:
 *   <script src="md.js"></script>
 *   <script>Valdr.loadMarkdownInto('content-id', 'thing.md');</script>
 */
(function () {
  function esc(s) {
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }
  function inline(s) {
    s = esc(s);
    s = s.replace(/`([^`]+)`/g, '<code>$1</code>');
    s = s.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
    s = s.replace(/\*([^*]+)\*/g, '<em>$1</em>');
    s = s.replace(/\[([^\]]+)\]\(([^)\s]+)\)/g, '<a href="$2">$1</a>');
    return s;
  }
  function cells(row) {
    return row.replace(/^\s*\||\|\s*$/g, '').split('|').map(function (c) { return c.trim(); });
  }
  function aligns(sep) {
    return cells(sep).map(function (c) {
      var l = c.charAt(0) === ':', r = c.charAt(c.length - 1) === ':';
      return r && l ? 'center' : (r ? 'right' : 'left');
    });
  }

  function render(md) {
    var lines = md.replace(/\r/g, '').split('\n');
    var out = [], depth = 0;
    function setDepth(d) {
      while (depth < d) { out.push('<ul>'); depth++; }
      while (depth > d) { out.push('</ul>'); depth--; }
    }
    for (var i = 0; i < lines.length; i++) {
      var line = lines[i];

      // table: a pipe row followed by a |---|---| separator row
      if (/^\s*\|(.+)\|\s*$/.test(line) && i + 1 < lines.length && /^\s*\|[\s:|-]+\|\s*$/.test(lines[i + 1])) {
        setDepth(0);
        var al = aligns(lines[i + 1]);
        var head = cells(line);
        out.push('<table class="md-table"><thead><tr>' + head.map(function (c, j) {
          return '<th style="text-align:' + (al[j] || 'left') + '">' + inline(c) + '</th>';
        }).join('') + '</tr></thead><tbody>');
        i += 2;
        while (i < lines.length && /^\s*\|(.+)\|\s*$/.test(lines[i])) {
          var rc = cells(lines[i]);
          out.push('<tr>' + rc.map(function (c, j) {
            return '<td style="text-align:' + (al[j] || 'left') + '">' + inline(c) + '</td>';
          }).join('') + '</tr>');
          i++;
        }
        out.push('</tbody></table>');
        i--;
        continue;
      }

      var h = line.match(/^(#{1,6})\s+(.*)$/);
      if (h) { setDepth(0); out.push('<h' + h[1].length + '>' + inline(h[2]) + '</h' + h[1].length + '>'); continue; }

      var li = line.match(/^(\s*)[-*]\s+(.*)$/);
      if (li) {
        var level = Math.floor(li[1].replace(/\t/g, '  ').length / 2) + 1;
        setDepth(level);
        var txt = li[2], task = txt.match(/^\[([ xX])\]\s+(.*)$/);
        if (task) out.push('<li class="' + (task[1].toLowerCase() === 'x' ? 'done' : 'todo') + '">' + inline(task[2]) + '</li>');
        else out.push('<li>' + inline(txt) + '</li>');
        continue;
      }

      if (line.trim() === '') { setDepth(0); continue; }
      setDepth(0);
      out.push('<p>' + inline(line) + '</p>');
    }
    setDepth(0);
    return out.join('\n');
  }

  function loadMarkdownInto(elId, url) {
    fetch(url, { cache: 'no-store' })
      .then(function (r) { return r.text(); })
      .then(function (md) { document.getElementById(elId).innerHTML = render(md); })
      .catch(function () { document.getElementById(elId).textContent = url + ' unavailable'; });
  }

  window.Valdr = { renderMarkdown: render, loadMarkdownInto: loadMarkdownInto };
})();
