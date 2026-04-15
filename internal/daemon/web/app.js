(function(){
'use strict';

// ─── Utilities ──────────────────────────────────────────────────────────────

function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;')}

function fmtSize(b){
  if(b==null||b<0)return'—';
  if(b>=1099511627776)return(b/1099511627776).toFixed(1)+' TB';
  if(b>=1073741824)return(b/1073741824).toFixed(1)+' GB';
  if(b>=1048576)return(b/1048576).toFixed(1)+' MB';
  if(b>=1024)return(b/1024).toFixed(1)+' KB';
  return b+' B';
}

function fmtDate(ts){
  if(!ts)return'—';
  var d=new Date(ts*1000),now=new Date(),diff=(now-d)/1000;
  if(diff<60)return'just now';
  if(diff<3600)return Math.floor(diff/60)+'m ago';
  if(diff<86400)return Math.floor(diff/3600)+'h ago';
  if(diff<604800)return Math.floor(diff/86400)+'d ago';
  if(d.getFullYear()===now.getFullYear())return d.toLocaleDateString(undefined,{month:'short',day:'numeric'});
  return d.toLocaleDateString(undefined,{year:'numeric',month:'short',day:'numeric'});
}

function fmtDateFull(ts){
  if(!ts)return'—';
  return new Date(ts*1000).toLocaleString();
}

function fmtUptime(s){
  if(s<60)return Math.floor(s)+'s';
  if(s<3600)return Math.floor(s/60)+'m '+Math.floor(s%60)+'s';
  var h=Math.floor(s/3600);
  return h+'h '+Math.floor((s%3600)/60)+'m';
}

function fileIcon(name,isDir){
  if(isDir)return'📁';
  var ext=(name.split('.').pop()||'').toLowerCase();
  if('mkv,mp4,avi,mov,wmv,m4v,ts,webm'.split(',').indexOf(ext)>-1)return'🎬';
  if('mp3,flac,aac,m4a,wav,ogg,opus'.split(',').indexOf(ext)>-1)return'🎵';
  if('jpg,jpeg,png,gif,webp,heic,bmp,svg,ico'.split(',').indexOf(ext)>-1)return'🖼️';
  if('zip,rar,7z,tar,gz,bz2,xz,zst'.split(',').indexOf(ext)>-1)return'📦';
  if(ext==='pdf')return'📄';
  if('doc,docx,xls,xlsx,ppt,pptx,odt,ods'.split(',').indexOf(ext)>-1)return'📝';
  if('py,go,js,ts,html,css,sh,rb,java,c,cpp,h,rs,swift,json,yaml,yml,toml,xml,sql,md'.split(',').indexOf(ext)>-1)return'💻';
  if('iso,dmg,img'.split(',').indexOf(ext)>-1)return'💿';
  return'📄';
}

function isImageFile(name){
  var ext=(name.split('.').pop()||'').toLowerCase();
  return'jpg,jpeg,png,gif,webp,bmp,svg,ico'.split(',').indexOf(ext)>-1;
}

function pathJoin(){
  var parts=Array.prototype.slice.call(arguments);
  var r=parts.join('/').replace(/\/+/g,'/');
  if(r.length>1&&r.endsWith('/'))r=r.slice(0,-1);
  return r||'/';
}

function pathParent(p){
  if(p==='/'||!p)return'/';
  var parts=p.replace(/\/$/,'').split('/');
  parts.pop();
  return parts.join('/')||'/';
}

function pathBasename(p){
  if(!p||p==='/')return'/';
  return p.replace(/\/$/,'').split('/').pop()||'';
}

// ─── API Client ─────────────────────────────────────────────────────────────

var api={
  _get:function(url){
    return fetch(url).then(function(r){
      if(!r.ok)return r.text().then(function(t){throw new Error(t)});
      return r.json();
    });
  },
  _post:function(url){
    return fetch(url,{method:'POST'}).then(function(r){
      if(!r.ok)return r.text().then(function(t){throw new Error(t)});
      return r.json();
    });
  },
  ls:function(p){return api._get('/api/ls?path='+encodeURIComponent(p||'/'))},
  status:function(){return api._get('/api/status')},
  health:function(){return api._get('/api/health')},
  remotes:function(){return api._get('/api/remotes')},
  uploads:function(){return api._get('/api/uploads')},
  metrics:function(){return api._get('/api/metrics')},
  info:function(p){return api._get('/api/info?path='+encodeURIComponent(p))},
  tree:function(p){return api._get('/api/tree?path='+encodeURIComponent(p||'/'))},
  find:function(pattern,root){return api._get('/api/find?pattern='+encodeURIComponent(pattern)+'&path='+encodeURIComponent(root||'/'))},
  du:function(p){return api._get('/api/du?path='+encodeURIComponent(p||'/'))},
  resync:function(){return api._post('/api/resync')},
  pin:function(p){return api._post('/api/pin?path='+encodeURIComponent(p))},
  unpin:function(p){return api._post('/api/unpin?path='+encodeURIComponent(p))},
  del:function(p){return api._post('/api/delete?path='+encodeURIComponent(p))},
  mv:function(src,dst){return api._post('/api/mv?src='+encodeURIComponent(src)+'&dst='+encodeURIComponent(dst))},
  mkdir:function(p){return api._post('/api/mkdir?path='+encodeURIComponent(p))},
  downloadUrl:function(p){return'/api/download?path='+encodeURIComponent(p)},
  upload:function(file,dir){
    var fd=new FormData();
    fd.append('file',file);
    fd.append('dir',dir||'/');
    return fetch('/api/upload',{method:'POST',body:fd}).then(function(r){
      if(!r.ok)return r.text().then(function(t){throw new Error(t)});
      return r.json();
    });
  },
  verify:function(p){
    return fetch('/api/verify?path='+encodeURIComponent(p)).then(function(r){
      return r.json();
    });
  },
  cancelUpload:function(p){return api._post('/api/upload/cancel?path='+encodeURIComponent(p))},
  activity:function(){return api._get('/api/activity')}
};

// ─── State ──────────────────────────────────────────────────────────────────

var S={
  page:'browse',
  path:'/',
  listing:null,
  selected:new Set(),
  sortCol:'name',
  sortAsc:true,
  focusIdx:-1,
  infoPanelPath:null,
  infoData:null,
  uploads:[],
  searchPattern:'',
  searchRoot:'/',
  searchResults:null,
  treeRoot:'/',
  treeData:null,
  loading:false
};

// ─── Toast ──────────────────────────────────────────────────────────────────

var toastId=0;
function toast(msg,type){
  type=type||'info';
  var id=++toastId;
  var el=document.getElementById('toasts');
  var d=document.createElement('div');
  d.className='toast toast-'+type;
  d.id='toast-'+id;
  d.textContent=msg;
  el.appendChild(d);
  setTimeout(function(){var t=document.getElementById('toast-'+id);if(t)t.remove()},4000);
}

// ─── Modal ──────────────────────────────────────────────────────────────────

var modalResolve=null;
function showModal(opts){
  // opts: {title, message, input, inputValue, inputPlaceholder, confirmLabel, danger}
  return new Promise(function(resolve){
    modalResolve=resolve;
    var overlay=document.getElementById('modal-overlay');
    var html='<div class="modal"><div class="modal-header"><h3>'+esc(opts.title)+'</h3>'
      +'<button class="btn-icon" data-action="modalClose" title="Close">✕</button></div>'
      +'<div class="modal-body">'+esc(opts.message||'');
    if(opts.input)html+='<input type="text" id="modal-input" value="'+esc(opts.inputValue||'')+'" placeholder="'+esc(opts.inputPlaceholder||'')+'" autofocus>';
    html+='</div><div class="modal-footer">'
      +'<button class="btn btn-secondary" data-action="modalClose">Cancel</button>'
      +'<button class="btn '+(opts.danger?'btn-danger':'btn-primary')+'" data-action="modalConfirm">'
      +esc(opts.confirmLabel||'Confirm')+'</button>'
      +'</div></div>';
    overlay.innerHTML=html;
    overlay.classList.add('open');
    var inp=document.getElementById('modal-input');
    if(inp){inp.focus();inp.select();}
  });
}

function closeModal(result){
  document.getElementById('modal-overlay').classList.remove('open');
  document.getElementById('modal-overlay').innerHTML='';
  if(modalResolve){modalResolve(result);modalResolve=null;}
}

// ─── Info Panel ─────────────────────────────────────────────────────────────

function openInfoPanel(p){
  S.infoPanelPath=p;
  S.infoData=null;
  var panel=document.getElementById('info-panel');
  var overlay=document.getElementById('panel-overlay');
  panel.innerHTML='<div class="panel-header"><h3>'+esc(pathBasename(p))+'</h3><button class="panel-close" data-action="closePanel">✕</button></div>'
    +'<div class="panel-body"><div class="loading-center"><span class="spinner"></span></div></div>';
  panel.classList.add('open');
  overlay.classList.add('open');
  api.info(p).then(function(data){
    S.infoData=data;
    renderInfoContent(data,panel);
  }).catch(function(e){
    panel.querySelector('.panel-body').innerHTML='<div class="empty-state"><p>'+esc(e.message)+'</p></div>';
  });
}

function renderInfoContent(data,panel){
  var body='';

  // Preview for images
  if(isImageFile(data.path)){
    body+='<img class="preview-img" src="'+esc(api.downloadUrl(data.path))+'" alt="preview" loading="lazy" onerror="this.style.display=\'none\'">';
  }

  body+='<div class="info-row"><span class="info-label">Path</span><span class="info-value">'+esc(data.path)+' <button class="btn-icon copy-btn" data-action="copyText" data-text="'+esc(data.path)+'" title="Copy">📋</button></span></div>';
  body+='<div class="info-row"><span class="info-label">Size</span><span class="info-value">'+esc(fmtSize(data.size_bytes))+'</span></div>';
  body+='<div class="info-row"><span class="info-label">Created</span><span class="info-value">'+esc(fmtDateFull(data.created_at))+'</span></div>';
  body+='<div class="info-row"><span class="info-label">Modified</span><span class="info-value">'+esc(fmtDateFull(data.modified_at))+'</span></div>';
  body+='<div class="info-row"><span class="info-label">State</span><span class="info-value">'+esc(data.upload_state)+'</span></div>';
  if(data.sha256){
    body+='<div class="info-row"><span class="info-label">SHA-256</span><span class="info-value" style="font-size:10px">'+esc(data.sha256)+' <button class="btn-icon copy-btn" data-action="copyText" data-text="'+esc(data.sha256)+'" title="Copy">📋</button></span></div>';
  }

  // Chunks
  if(data.chunks&&data.chunks.length){
    body+='<div style="margin-top:16px"><span class="info-label">Chunks ('+data.chunks.length+')</span>'
      +'<table class="chunk-table"><thead><tr><th>#</th><th>Size</th><th>Encrypted</th><th>Providers</th></tr></thead><tbody>';
    data.chunks.forEach(function(c){
      body+='<tr><td>'+c.sequence+'</td><td>'+fmtSize(c.size_bytes)+'</td><td>'+fmtSize(c.encrypted_size)+'</td>'
        +'<td>'+esc((c.providers||[]).join(', '))+'</td></tr>';
    });
    body+='</tbody></table></div>';
  }

  panel.querySelector('.panel-body').innerHTML=body;

  // Actions
  var actions='<button class="btn btn-primary" data-action="download" data-path="'+esc(data.path)+'">⬇ Download</button>'
    +'<button class="btn btn-secondary" data-action="verifyFile" data-path="'+esc(data.path)+'">✓ Verify</button>';
  if(data.upload_state==='local'||data.upload_state==='uploading'){
    actions+='<button class="btn btn-secondary" data-action="unpin" data-path="'+esc(data.path)+'">☁️ Unpin</button>';
  } else {
    actions+='<button class="btn btn-secondary" data-action="pin" data-path="'+esc(data.path)+'">📌 Pin</button>';
  }
  actions+='<button class="btn btn-secondary" data-action="moveFile" data-path="'+esc(data.path)+'">📁 Move</button>'
    +'<button class="btn btn-danger" data-action="deleteFile" data-path="'+esc(data.path)+'">🗑 Delete</button>';

  var existing=panel.querySelector('.panel-actions');
  if(existing)existing.remove();
  var actionsDiv=document.createElement('div');
  actionsDiv.className='panel-actions';
  actionsDiv.innerHTML=actions;
  panel.appendChild(actionsDiv);
}

function closeInfoPanel(){
  S.infoPanelPath=null;
  S.infoData=null;
  document.getElementById('info-panel').classList.remove('open');
  document.getElementById('panel-overlay').classList.remove('open');
}

// ─── Selection Bar ──────────────────────────────────────────────────────────

function updateSelectionBar(){
  var bar=document.getElementById('selection-bar');
  var n=S.selected.size;
  if(n===0){
    bar.classList.remove('visible');
    return;
  }
  bar.classList.add('visible');
  bar.innerHTML='<span class="sel-count">'+n+' selected</span>'
    +'<span class="sel-divider"></span>'
    +'<button class="btn btn-secondary" data-action="bulkPin">📌 Pin</button>'
    +'<button class="btn btn-secondary" data-action="bulkUnpin">☁️ Unpin</button>'
    +'<button class="btn btn-secondary" data-action="bulkDownload">⬇ Download</button>'
    +'<button class="btn btn-danger" data-action="bulkDelete">🗑 Delete</button>'
    +'<span class="sel-divider"></span>'
    +'<button class="btn btn-ghost" data-action="clearSelection">✕ Clear</button>';
}

// ─── Sorting ────────────────────────────────────────────────────────────────

function sortItems(dirs,files){
  var d=(dirs||[]).slice().sort();
  var f=(files||[]).slice();
  var col=S.sortCol,asc=S.sortAsc;
  f.sort(function(a,b){
    var va,vb;
    if(col==='name'){va=a.name.toLowerCase();vb=b.name.toLowerCase();return asc?va.localeCompare(vb):vb.localeCompare(va)}
    if(col==='size'){va=a.size;vb=b.size;return asc?va-vb:vb-va}
    if(col==='state'){va=a.local_state||'';vb=b.local_state||'';return asc?va.localeCompare(vb):vb.localeCompare(va)}
    if(col==='modified'){va=a.modified_at||0;vb=b.modified_at||0;return asc?va-vb:vb-va}
    return 0;
  });
  return{dirs:d,files:f};
}

function sortIndicator(col){
  if(S.sortCol!==col)return'';
  return' <span class="sort-indicator">'+(S.sortAsc?'▲':'▼')+'</span>';
}

// ─── Page: Browse ───────────────────────────────────────────────────────────

function renderBrowse(){
  var html='';
  // Breadcrumb
  html+='<div class="breadcrumb">';
  html+='<a class="bc-item" data-action="navigate" data-path="/">pdrive</a>';
  if(S.path&&S.path!=='/'){
    var parts=S.path.replace(/^\//,'').replace(/\/$/,'').split('/');
    var built='';
    for(var i=0;i<parts.length;i++){
      built+='/'+parts[i];
      html+='<span class="bc-sep">/</span>';
      if(i===parts.length-1){
        html+='<span class="bc-current">'+esc(parts[i])+'</span>';
      } else {
        html+='<a class="bc-item" data-action="navigate" data-path="'+esc(built)+'">'+esc(parts[i])+'</a>';
      }
    }
  }
  html+='</div>';

  // Action bar
  html+='<div class="action-bar">';
  html+='<button class="btn btn-secondary" data-action="newFolder">+ New Folder</button>';
  html+='<button class="btn btn-secondary" data-action="uploadFile">⬆ Upload Files</button>';
  html+='<button class="btn btn-secondary" data-action="uploadFolder">📁 Upload Folder</button>';
  html+='<input type="file" id="upload-input" style="display:none" multiple>';
  html+='<input type="file" id="upload-folder-input" style="display:none" webkitdirectory>';
  html+='<div style="flex:1"></div>';
  html+='<button class="btn-ghost" data-action="duHere" title="Disk usage" style="font-size:12px">💾 Usage</button>';
  html+='<button class="btn-ghost" data-action="refreshBrowse" title="Refresh" style="font-size:12px">🔄 Refresh</button>';
  if(S.path!=='/'){
    html+='<button class="btn-ghost" data-action="navigate" data-path="'+esc(pathParent(S.path))+'" title="Go up">⬆ Up</button>';
  }
  html+='</div>';

  // File table
  if(S.loading&&!S.listing){
    html+='<div class="loading-center"><span class="spinner"></span> Loading…</div>';
  } else if(S.listing){
    var sorted=sortItems(S.listing.dirs,S.listing.files);
    var dirs=sorted.dirs,files=sorted.files;

    if(!dirs.length&&!files.length){
      html+='<div class="empty-state"><div class="empty-icon">📭</div><p>This directory is empty</p>'
        +'<p style="font-size:12px;color:var(--fg3);margin-top:4px">Drag &amp; drop files here, or use the buttons above to upload</p>'
        +'<button class="btn btn-primary" data-action="uploadFile" style="margin-top:12px">⬆ Upload Files</button></div>';
    } else {
      html+='<div class="file-table-wrap"><table class="file-table"><thead><tr>';
      html+='<th class="cell-check"><input type="checkbox" data-action="selectAll" title="Select all"></th>';
      html+='<th data-action="sort" data-col="name">Name'+sortIndicator('name')+'</th>';
      html+='<th class="r" data-action="sort" data-col="size" style="width:90px">Size'+sortIndicator('size')+'</th>';
      html+='<th data-action="sort" data-col="state" style="width:90px">State'+sortIndicator('state')+'</th>';
      html+='<th class="r" data-action="sort" data-col="modified" style="width:110px">Modified'+sortIndicator('modified')+'</th>';
      html+='<th style="width:80px"></th>';
      html+='</tr></thead><tbody>';

      dirs.forEach(function(d){
        var fp=pathJoin(S.path,d);
        var sel=S.selected.has(fp);
        html+='<tr class="file-row'+(sel?' selected':'')+'" data-filepath="'+esc(fp)+'" data-isdir="1">';
        html+='<td class="cell-check"><input type="checkbox" data-action="selectFile" data-path="'+esc(fp)+'"'+(sel?' checked':'')+'></td>';
        html+='<td class="cell-name"><a data-action="navigate" data-path="'+esc(fp)+'"><span class="file-icon">📁</span>'+esc(d)+'</a></td>';
        html+='<td class="cell-size r">—</td><td></td><td class="cell-date r">—</td>';
        html+='<td class="cell-actions"><button class="btn-icon" data-action="deleteFile" data-path="'+esc(fp)+'" title="Delete">🗑</button></td>';
        html+='</tr>';
      });

      files.forEach(function(f){
        var fp=f.path;
        var sel=S.selected.has(fp);
        var focused=false;
        var icon=fileIcon(f.name,false);
        var stateClass=f.local_state==='local'?'badge-local':f.local_state==='uploading'?'badge-uploading':'badge-stub';
        var stateLabel=f.local_state==='local'?'local':f.local_state==='uploading'?'uploading':'cloud';

        html+='<tr class="file-row'+(sel?' selected':'')+'" data-filepath="'+esc(fp)+'" data-state="'+esc(f.local_state||'stub')+'">';
        html+='<td class="cell-check"><input type="checkbox" data-action="selectFile" data-path="'+esc(fp)+'"'+(sel?' checked':'')+'></td>';
        html+='<td class="cell-name"><a data-action="showInfo" data-path="'+esc(fp)+'"><span class="file-icon">'+icon+'</span>'+esc(f.name)+'</a></td>';
        html+='<td class="cell-size r">'+fmtSize(f.size)+'</td>';
        html+='<td><span class="badge '+stateClass+'"><span class="badge-dot"></span>'+stateLabel+'</span></td>';
        html+='<td class="cell-date r">'+fmtDate(f.modified_at)+'</td>';
        html+='<td class="cell-actions">'
          +'<button class="btn-icon" data-action="download" data-path="'+esc(fp)+'" title="Download">⬇</button>'
          +'<button class="btn-icon" data-action="showInfo" data-path="'+esc(fp)+'" title="Info">ℹ</button>'
          +'</td>';
        html+='</tr>';
      });
      html+='</tbody></table></div>';

      // Browse footer with counts
      html+='<div class="browse-footer">'+dirs.length+' folder'+(dirs.length!==1?'s':'')+', '+files.length+' file'+(files.length!==1?'s':'')+'</div>';
    }
  }
  return html;
}

function loadBrowse(p){
  p=p||S.path||'/';
  S.path=p;
  S.loading=true;
  S.selected.clear();
  updateSelectionBar();
  renderPage();
  api.ls(p).then(function(data){
    S.listing=data;
    S.path=data.path||p;
    S.loading=false;
    renderPage();
  }).catch(function(e){
    S.loading=false;
    S.listing={path:p,dirs:[],files:[]};
    renderPage();
    toast('Failed to load directory: '+e.message,'error');
  });
}

// ─── Page: Dashboard ────────────────────────────────────────────────────────

function renderDashboard(){
  var html='<div class="page"><div style="display:flex;align-items:center;gap:12px;margin-bottom:16px"><h2 class="page-title" style="margin:0">Dashboard</h2>'
    +'<button class="btn btn-secondary" data-action="resyncProviders" style="font-size:12px">🔄 Resync Providers</button></div>';
  html+='<div id="dash-health"><div class="loading-center"><span class="spinner"></span></div></div>';
  html+='<div id="dash-storage"><div class="loading-center"><span class="spinner"></span></div></div>';
  html+='<div class="section-title">Storage Providers</div>';
  html+='<div id="dash-providers"><div class="loading-center"><span class="spinner"></span></div></div>';
  html+='</div>';
  return html;
}

// Provider color palette (distinct, accessible)
var providerColors=['#3b82f6','#f59e0b','#10b981','#8b5cf6','#ec4899','#06b6d4','#f97316','#6366f1'];

function loadDashboard(){
  renderPage();
  Promise.all([api.health(),api.status(),api.remotes().catch(function(){return{remotes:[]}})]).then(function(results){
    var health=results[0],status=results[1],remotes=results[2];

    // Health
    var hEl=document.getElementById('dash-health');
    if(hEl){
      var hClass=health.status==='ok'?'ok':'degraded';
      hEl.innerHTML='<div class="card-grid">'
        +'<div class="card"><div class="card-title">Health</div><div class="card-value"><span class="health-dot '+hClass+'"></span> '+esc(health.status)+'</div>'
        +'<div class="card-sub">Uptime: '+fmtUptime(health.uptime_seconds)+' · In-flight: '+health.in_flight_uploads+' · DB: '+(health.db_ok?'✓':'✗')+'</div></div>'
        +'<div class="card"><div class="card-title">Total Files</div><div class="card-value">'+status.total_files.toLocaleString()+'</div>'
        +'<div class="card-sub">'+fmtSize(status.total_bytes)+' across all providers</div></div>'
        +'</div>';
    }

    // Storage overview
    var sEl=document.getElementById('dash-storage');
    if(!sEl)return;
    var providers=status.providers||[];
    if(!providers.length){sEl.innerHTML='';return}

    var combTotal=0,combUsed=0,combPd=0;
    providers.forEach(function(p){
      if(p.quota_total_bytes)combTotal+=p.quota_total_bytes;
      if(p.quota_total_bytes&&p.quota_free_bytes!=null)combUsed+=(p.quota_total_bytes-p.quota_free_bytes);
      if(p.quota_used_by_pdrive)combPd+=p.quota_used_by_pdrive;
    });
    var combFree=combTotal-combUsed;
    var combPct=combTotal>0?Math.round(combUsed/combTotal*100):0;

    // Donut chart via SVG
    var pdPct=combTotal>0?combPd/combTotal*100:0;
    var otherUsed=combUsed-combPd;
    var otherPct=combTotal>0?otherUsed/combTotal*100:0;
    var r=52,cx=60,cy=60,circ=2*Math.PI*r;
    var pdLen=circ*pdPct/100,otherLen=circ*otherPct/100;
    var donutSvg='<svg class="storage-donut-ring" viewBox="0 0 120 120">'
      +'<circle cx="'+cx+'" cy="'+cy+'" r="'+r+'" fill="none" stroke="var(--bg3)" stroke-width="12"/>';
    if(pdPct>0)donutSvg+='<circle cx="'+cx+'" cy="'+cy+'" r="'+r+'" fill="none" stroke="'+providerColors[0]+'" stroke-width="12" stroke-dasharray="'+pdLen+' '+circ+'" stroke-dashoffset="0" stroke-linecap="round"/>';
    if(otherPct>0)donutSvg+='<circle cx="'+cx+'" cy="'+cy+'" r="'+r+'" fill="none" stroke="var(--fg3)" stroke-width="12" stroke-dasharray="'+otherLen+' '+circ+'" stroke-dashoffset="-'+pdLen+'" stroke-linecap="round" opacity=".5"/>';
    donutSvg+='</svg>';

    // Combined stacked bar — one segment per provider (colored)
    var barHtml='';
    var legendHtml='';
    providers.forEach(function(p,i){
      var c=providerColors[i%providerColors.length];
      var pUsedByPd=p.quota_used_by_pdrive||0;
      var pPct=combTotal>0?Math.max(0,pUsedByPd/combTotal*100):0;
      if(pPct>0)barHtml+='<div class="bar-seg" style="width:'+pPct+'%;background:'+c+'" title="'+esc(p.name)+': '+fmtSize(pUsedByPd)+'"></div>';
      legendHtml+='<div class="storage-legend-item"><div class="storage-legend-dot" style="background:'+c+'"></div><span>'+esc(p.name)+'</span> <span style="color:var(--fg3)">'+fmtSize(pUsedByPd)+'</span></div>';
    });
    // "Other" segment (non-pdrive usage across all providers)
    if(otherUsed>0){
      var oSPct=combTotal>0?Math.max(0,otherUsed/combTotal*100):0;
      barHtml+='<div class="bar-seg" style="width:'+oSPct+'%;background:var(--fg3);opacity:.35" title="Other usage: '+fmtSize(otherUsed)+'"></div>';
      legendHtml+='<div class="storage-legend-item"><div class="storage-legend-dot" style="background:var(--fg3);opacity:.4"></div><span>Other</span> <span style="color:var(--fg3)">'+fmtSize(otherUsed)+'</span></div>';
    }
    legendHtml+='<div class="storage-legend-item"><div class="storage-legend-dot" style="background:var(--bg3)"></div><span>Free</span> <span style="color:var(--fg3)">'+fmtSize(combFree)+'</span></div>';

    sEl.innerHTML='<div class="storage-overview">'
      +'<div class="storage-top">'
      +'<div class="storage-donut">'+donutSvg
      +'<div class="storage-donut-center"><div class="storage-donut-pct">'+combPct+'%</div><div class="storage-donut-label">used</div></div>'
      +'</div>'
      +'<div class="storage-info">'
      +'<h3>Combined Storage</h3>'
      +'<div class="storage-info-value">'+fmtSize(combFree)+' free</div>'
      +'<div class="storage-info-sub">'+fmtSize(combPd)+' pdrive · '+fmtSize(combUsed)+' total used · '+fmtSize(combTotal)+' capacity</div>'
      +'</div></div>'
      +'<div class="storage-combined-bar">'+barHtml+'</div>'
      +'<div class="storage-legend">'+legendHtml+'</div>'
      +'</div>';

    // Provider cards
    var pEl=document.getElementById('dash-providers');
    if(!pEl)return;
    var cards='<div class="card-grid">';
    providers.forEach(function(p,i){
      var c=providerColors[i%providerColors.length];
      var total=p.quota_total_bytes||0;
      var free=p.quota_free_bytes||0;
      var used=total>0?total-free:0;
      var usedByPd=p.quota_used_by_pdrive||0;
      var otherP=used-usedByPd;
      var pdPctP=total>0?Math.min(100,usedByPd/total*100):0;
      var otherPctP=total>0?Math.min(100,otherP/total*100):0;
      var totalPctP=Math.round(pdPctP+otherPctP);
      cards+='<div class="provider-card"><div class="provider-header"><div><div class="provider-name">'+esc(p.name);
      if(p.type)cards+=' <span class="provider-type">('+esc(p.type)+')</span>';
      cards+='</div>';
      if(p.account_identity)cards+='<div class="provider-account">'+esc(p.account_identity)+'</div>';
      cards+='</div><div class="provider-pct" style="color:'+c+'">'+totalPctP+'<span class="provider-pct-label">% used</span></div></div>';
      cards+='<div class="provider-free">'+fmtSize(free)+' free</div>';
      cards+='<div class="provider-bar">';
      if(pdPctP>0)cards+='<div class="bar-seg" style="width:'+pdPctP+'%;background:'+c+'"></div>';
      if(otherPctP>0)cards+='<div class="bar-seg" style="width:'+otherPctP+'%;background:var(--fg3);opacity:.35"></div>';
      cards+='</div>';
      cards+='<div class="provider-detail"><span style="color:'+c+'">'+fmtSize(usedByPd)+' pdrive</span><span>'+fmtSize(otherP)+' other</span><span>'+fmtSize(total)+' total</span></div></div>';
    });
    cards+='</div>';
    pEl.innerHTML=cards;

  }).catch(function(e){
    toast('Failed to load dashboard: '+e.message,'error');
  });
}

// ─── Page: Uploads ──────────────────────────────────────────────────────────

function renderUploads(){
  var html='<div class="page"><h2 class="page-title">Uploads</h2>';
  html+='<div id="uploads-list"></div></div>';
  return html;
}

function loadUploads(){
  renderPage();
  refreshUploads();
}

function refreshUploads(){
  api.uploads().then(function(ups){
    S.uploads=ups||[];
    var badge=document.getElementById('upload-badge');
    if(badge){
      if(S.uploads.length){badge.style.display='';badge.textContent=S.uploads.length;}
      else{badge.style.display='none';}
    }
    var el=document.getElementById('uploads-list');
    if(!el)return;
    if(!S.uploads.length){
      el.innerHTML='<div class="empty-state"><div class="empty-icon">✨</div><p>No active uploads</p></div>';
      return;
    }
    var html='';
    S.uploads.forEach(function(u,i){
      var pct=u.BytesTotal>0?Math.min(100,Math.round(u.BytesDone/u.BytesTotal*100)):
               (u.TotalChunks>0?Math.min(100,Math.round(u.ChunksUploaded/u.TotalChunks*100)):0);
      var name=u.VirtualPath.split('/').pop();
      var dir=u.VirtualPath.substring(0,u.VirtualPath.lastIndexOf('/'));
      var statusLabel=u.Failed?'✗ failed':u.Preparing?'Preparing…':'';
      html+='<div class="upload-card">'
        +'<div class="upload-card-header">'
        +'<div class="upload-name'+(u.Failed?' fail':'')+'">'+esc(name)+(statusLabel?' '+statusLabel:'')+'</div>'
        +(!u.Failed?'<button class="btn btn-danger upload-cancel-btn" data-cancel-path="'+esc(u.VirtualPath)+'" title="Cancel upload">✕</button>':'')
        +'</div>';
      if(dir)html+='<div class="upload-dir">'+esc(dir)+'</div>';
      if(u.Preparing){
        html+='<div class="upload-bar"><div class="upload-bar-fill preparing"></div></div>'
          +'<div class="upload-meta"><span>Preparing… hashing file</span>'
          +'<span>'+fmtSize(u.SizeBytes)+'</span></div></div>';
      } else {
        var speed=u.SpeedBPS>0?fmtSize(u.SpeedBPS)+'/s':'';
        var eta='';
        if(u.SpeedBPS>0&&u.BytesTotal>u.BytesDone){
          var secs=Math.round((u.BytesTotal-u.BytesDone)/u.SpeedBPS);
          if(secs<60)eta=secs+'s';
          else if(secs<3600)eta=Math.floor(secs/60)+'m '+secs%60+'s';
          else eta=Math.floor(secs/3600)+'h '+Math.floor((secs%3600)/60)+'m';
        }
        var detail=pct+'%';
        if(u.BytesTotal>0)detail+=' · '+fmtSize(u.BytesDone)+' / '+fmtSize(u.BytesTotal);
        if(speed)detail+=' · '+speed;
        if(eta)detail+=' · ~'+eta;
        html+='<div class="upload-bar"><div class="upload-bar-fill'+(u.Failed?' fail':'')+'" style="width:'+pct+'%"></div></div>'
          +'<div class="upload-meta"><span>'+detail+'</span>'
          +'<span>'+fmtSize(u.SizeBytes)+'</span></div></div>';
      }
    });
    el.innerHTML=html;
    // Wire cancel buttons
    el.querySelectorAll('.upload-cancel-btn').forEach(function(btn){
      btn.addEventListener('click',function(e){
        e.stopPropagation();
        var vp=btn.getAttribute('data-cancel-path');
        if(!vp)return;
        btn.disabled=true;
        btn.textContent='…';
        api.cancelUpload(vp).then(function(){
          toast('Cancelling upload: '+vp.split('/').pop(),'info');
        }).catch(function(err){
          toast('Cancel failed: '+err.message,'error');
        });
      });
    });
  }).catch(function(){});
}

// ─── Page: Search ───────────────────────────────────────────────────────────

function renderSearch(){
  var html='<div class="page"><h2 class="page-title">Search</h2>';
  html+='<div class="search-form" id="search-form">'
    +'<input type="text" id="search-pattern" placeholder="Pattern (e.g. *.jpg, report*)" value="'+esc(S.searchPattern)+'">'
    +'<input type="text" id="search-root" placeholder="Root path" value="'+esc(S.searchRoot)+'" style="max-width:150px">'
    +'<button class="btn btn-primary" data-action="doSearch">Search</button></div>';
  html+='<div id="search-results">';
  if(S.searchResults!==null){
    if(!S.searchResults.length){
      html+='<div class="empty-state"><p>No files found</p></div>';
    } else {
      html+='<table class="file-table"><thead><tr><th>Name</th><th>Path</th><th class="r">Size</th><th style="width:100px"></th></tr></thead><tbody>';
      S.searchResults.forEach(function(f){
        var name=pathBasename(f.path);
        html+='<tr class="file-row"><td class="cell-name"><a data-action="showInfo" data-path="'+esc(f.path)+'">'
          +'<span class="file-icon">'+fileIcon(name,false)+'</span>'+esc(name)+'</a></td>'
          +'<td style="font-size:11px;color:var(--fg2)">'+esc(f.path)+'</td>'
          +'<td class="r cell-size">'+fmtSize(f.size)+'</td>'
          +'<td class="cell-actions" style="opacity:1">'
          +'<button class="btn-icon" data-action="download" data-path="'+esc(f.path)+'" title="Download">⬇</button>'
          +'<button class="btn-icon" data-action="navigate" data-path="'+esc(pathParent(f.path))+'" title="Open in folder">📂</button></td></tr>';
      });
      html+='</tbody></table>';
      html+='<div style="padding:8px 0;font-size:11px;color:var(--fg3)">'+S.searchResults.length+' result(s)</div>';
    }
  }
  html+='</div></div>';
  return html;
}

function doSearch(){
  var patternEl=document.getElementById('search-pattern');
  var rootEl=document.getElementById('search-root');
  if(!patternEl)return;
  var pattern=patternEl.value.trim();
  var root=rootEl?rootEl.value.trim():'/';
  if(!pattern){toast('Enter a search pattern','error');return}
  S.searchPattern=pattern;
  S.searchRoot=root||'/';
  var el=document.getElementById('search-results');
  if(el)el.innerHTML='<div class="loading-center"><span class="spinner"></span> Searching…</div>';
  api.find(pattern,root).then(function(results){
    S.searchResults=results||[];
    renderPage();
  }).catch(function(e){
    toast('Search failed: '+e.message,'error');
    S.searchResults=[];
    renderPage();
  });
}

// ─── Page: Tree ─────────────────────────────────────────────────────────────

function renderTree(){
  var html='<div class="page"><h2 class="page-title">Tree</h2>';
  html+='<div style="display:flex;gap:8px;margin-bottom:12px">'
    +'<input type="text" id="tree-root" placeholder="Root path" value="'+esc(S.treeRoot)+'" style="max-width:200px">'
    +'<button class="btn btn-secondary" data-action="doTree">Load Tree</button></div>';
  html+='<div id="tree-content">';
  if(S.treeData!==null){
    if(!S.treeData.length){
      html+='<div class="empty-state"><p>Empty tree</p></div>';
    } else {
      html+=buildTree(S.treeData,S.treeRoot);
    }
  }
  html+='</div></div>';
  return html;
}

function buildTree(items,root){
  // Build a nested structure from flat list
  var tree={};
  var totalSize=0;
  items.forEach(function(f){
    // Remove root prefix
    var rel=f.path;
    if(root!=='/'&&rel.startsWith(root))rel=rel.substring(root.length);
    if(rel.startsWith('/'))rel=rel.substring(1);
    var parts=rel.split('/');
    var node=tree;
    for(var i=0;i<parts.length-1;i++){
      if(!node[parts[i]])node[parts[i]]={__children:{}};
      node=node[parts[i]].__children||(node[parts[i]].__children={});
    }
    var fname=parts[parts.length-1];
    node[fname]={__file:true,__size:f.size,__path:f.path};
    totalSize+=f.size||0;
  });

  var html='<div style="font-family:\'SF Mono\',Menlo,Monaco,monospace;font-size:12px;line-height:1.8">';
  html+=renderTreeNode(tree,'',true);
  html+='</div>';
  html+='<div style="padding:10px 0;font-size:11px;color:var(--fg3)">'+items.length+' file(s), '+fmtSize(totalSize)+' total</div>';
  return html;
}

function renderTreeNode(node,prefix,isRoot){
  var html='';
  var keys=Object.keys(node).sort(function(a,b){
    var aDir=node[a].__children!==undefined&&!node[a].__file;
    var bDir=node[b].__children!==undefined&&!node[b].__file;
    if(aDir&&!bDir)return -1;
    if(!aDir&&bDir)return 1;
    return a.localeCompare(b);
  });
  keys.forEach(function(k,i){
    if(k.startsWith('__'))return;
    var entry=node[k];
    var isLast=i===keys.length-1;
    var connector=isRoot?'':( isLast?'└── ':'├── ');
    var nextPrefix=isRoot?'':(prefix+(isLast?'    ':'│   '));

    if(entry.__file){
      html+='<div class="tree-node"><span class="tree-indent">'+esc(prefix+connector)+'</span>'
        +'<a data-action="showInfo" data-path="'+esc(entry.__path)+'">'+fileIcon(k,false)+' '+esc(k)+'</a>'
        +'<span class="tree-size">'+fmtSize(entry.__size)+'</span></div>';
    } else {
      html+='<div class="tree-node"><span class="tree-indent">'+esc(prefix+connector)+'</span>'
        +'<span>📁 <strong>'+esc(k)+'</strong></span></div>';
      if(entry.__children){
        html+=renderTreeNode(entry.__children,nextPrefix,false);
      }
    }
  });
  return html;
}

function loadTree(){
  var rootEl=document.getElementById('tree-root');
  var root=rootEl?rootEl.value.trim():'/';
  S.treeRoot=root||'/';
  var el=document.getElementById('tree-content');
  if(el)el.innerHTML='<div class="loading-center"><span class="spinner"></span> Loading tree…</div>';
  api.tree(root).then(function(data){
    S.treeData=data||[];
    renderPage();
  }).catch(function(e){
    toast('Failed to load tree: '+e.message,'error');
    S.treeData=[];
    renderPage();
  });
}

// ─── Page: Metrics ──────────────────────────────────────────────────────────

function renderMetrics(){
  var html='<div class="page"><h2 class="page-title">Metrics</h2>';
  html+='<div id="metrics-content"><div class="loading-center"><span class="spinner"></span></div></div></div>';
  return html;
}

function loadMetrics(){
  renderPage();
  api.metrics().then(function(m){
    var el=document.getElementById('metrics-content');
    if(!el)return;
    el.innerHTML='<div class="card-grid">'
      +'<div class="card"><div class="card-title">Files Uploaded</div><div class="card-value">'+m.files_uploaded+'</div></div>'
      +'<div class="card"><div class="card-title">Files Downloaded</div><div class="card-value">'+m.files_downloaded+'</div></div>'
      +'<div class="card"><div class="card-title">Files Deleted</div><div class="card-value">'+m.files_deleted+'</div></div>'
      +'<div class="card"><div class="card-title">Chunks Uploaded</div><div class="card-value">'+m.chunks_uploaded+'</div></div>'
      +'<div class="card"><div class="card-title">Bytes Uploaded</div><div class="card-value">'+fmtSize(m.bytes_uploaded)+'</div></div>'
      +'<div class="card"><div class="card-title">Bytes Downloaded</div><div class="card-value">'+fmtSize(m.bytes_downloaded)+'</div></div>'
      +'<div class="card"><div class="card-title">Dedup Hits</div><div class="card-value">'+m.dedup_hits+'</div></div>'
      +'</div>';
  }).catch(function(e){
    toast('Failed to load metrics: '+e.message,'error');
  });
}

// ─── Page: Activity ─────────────────────────────────────────────────────────

function renderActivity(){
  var html='<div class="page"><h2 class="page-title">Activity</h2>';
  html+='<div id="activity-content"><div class="loading-center"><span class="spinner"></span></div></div></div>';
  return html;
}

function loadActivity(){
  renderPage();
  api.activity().then(function(items){
    var el=document.getElementById('activity-content');
    if(!el)return;
    if(!items||!items.length){
      el.innerHTML='<div class="empty-state"><div class="empty-icon">📋</div><p>No activity yet</p></div>';
      return;
    }
    var html='<table class="file-table"><thead><tr><th>Action</th><th>Path</th><th>Detail</th><th class="r">Time</th></tr></thead><tbody>';
    items.forEach(function(e){
      html+='<tr><td><span class="badge badge-local"><span class="badge-dot"></span>'+esc(e.action)+'</span></td>';
      html+='<td>'+esc(e.path)+'</td>';
      html+='<td>'+esc(e.detail||'')+'</td>';
      html+='<td class="r">'+fmtDateFull(e.created_at)+'</td></tr>';
    });
    html+='</tbody></table>';
    el.innerHTML=html;
  }).catch(function(e){
    toast('Failed to load activity: '+e.message,'error');
  });
}

// ─── Page: Logs ─────────────────────────────────────────────────────────────

var _logWS=null;
var _logAutoScroll=true;
var _logLevel='';

function renderLogs(){
  var html='<div class="page"><h2 class="page-title">Logs</h2>';
  html+='<div class="logs-toolbar" style="display:flex;gap:8px;align-items:center;margin-bottom:12px;flex-wrap:wrap">';
  html+='<select id="log-level-filter" style="padding:4px 8px;border-radius:4px;border:1px solid var(--border);background:var(--bg-card);color:var(--fg)">';
  html+='<option value="">All Levels</option>';
  html+='<option value="DEBUG">DEBUG</option>';
  html+='<option value="INFO">INFO</option>';
  html+='<option value="WARN">WARN</option>';
  html+='<option value="ERROR">ERROR</option>';
  html+='</select>';
  html+='<label style="display:flex;align-items:center;gap:4px;font-size:0.85em;cursor:pointer"><input type="checkbox" id="log-autoscroll" checked> Auto-scroll</label>';
  html+='<button class="btn btn-secondary" id="log-clear" style="margin-left:auto;font-size:0.8em">Clear</button>';
  html+='<span id="log-status" style="font-size:0.75em;color:var(--fg-dim)"></span>';
  html+='</div>';
  html+='<div id="log-entries" style="font-family:\'SF Mono\',Menlo,Consolas,monospace;font-size:0.8em;line-height:1.6;overflow-y:auto;max-height:calc(100vh - 220px);background:var(--bg-card);border:1px solid var(--border);border-radius:6px;padding:8px 12px"></div>';
  html+='</div>';
  return html;
}

function _logLevelClass(level){
  switch(level){
    case'DEBUG':return'color:var(--fg-dim)';
    case'INFO':return'color:var(--accent)';
    case'WARN':return'color:#f0ad4e';
    case'ERROR':return'color:#d9534f;font-weight:600';
    default:return'';
  }
}

function _formatLogEntry(e){
  var t=e.time?new Date(e.time).toLocaleTimeString():'';
  var lvl=e.level||'INFO';
  if(_logLevel&&lvl!==_logLevel)return'';
  var style=_logLevelClass(lvl);
  var attrs='';
  if(e.attrs){
    var keys=Object.keys(e.attrs);
    for(var i=0;i<keys.length;i++){
      attrs+=' <span style="color:var(--fg-dim)">'+esc(keys[i])+'</span>='+esc(String(e.attrs[keys[i]]));
    }
  }
  return'<div class="log-line" style="'+style+'"><span style="color:var(--fg-dim)">'+esc(t)+'</span> <span style="font-weight:600;min-width:48px;display:inline-block">'+esc(lvl)+'</span> '+esc(e.msg||'')+attrs+'</div>';
}

function _connectLogStream(){
  if(_logWS)return;
  var prot=location.protocol==='https:'?'wss:':'ws:';
  var url=prot+'//'+location.host+'/api/logs/stream';
  var statusEl=document.getElementById('log-status');
  if(statusEl)statusEl.textContent='Connecting…';
  _logWS=new WebSocket(url);
  _logWS.onopen=function(){
    if(statusEl)statusEl.textContent='🟢 Live';
  };
  _logWS.onmessage=function(evt){
    try{
      var e=JSON.parse(evt.data);
      var html=_formatLogEntry(e);
      if(!html)return;
      var el=document.getElementById('log-entries');
      if(!el)return;
      el.insertAdjacentHTML('beforeend',html);
      // Keep buffer reasonable
      while(el.children.length>2000)el.removeChild(el.firstChild);
      if(_logAutoScroll)el.scrollTop=el.scrollHeight;
    }catch(ex){/* ignore parse errors */}
  };
  _logWS.onclose=function(){
    _logWS=null;
    if(statusEl)statusEl.textContent='🔴 Disconnected';
    // Reconnect if still on logs page
    if(S.page==='logs')setTimeout(_connectLogStream,2000);
  };
  _logWS.onerror=function(){
    if(_logWS)_logWS.close();
  };
}

function _disconnectLogStream(){
  if(_logWS){_logWS.onclose=null;_logWS.close();_logWS=null;}
}

function loadLogs(){
  renderPage();
  var el=document.getElementById('log-entries');
  if(!el)return;
  el.innerHTML='<div class="loading-center"><span class="spinner"></span></div>';

  // Wire up controls
  var levelSel=document.getElementById('log-level-filter');
  if(levelSel){
    levelSel.value=_logLevel;
    levelSel.onchange=function(){_logLevel=this.value;_refilterLogs()};
  }
  var autoChk=document.getElementById('log-autoscroll');
  if(autoChk){autoChk.checked=_logAutoScroll;autoChk.onchange=function(){_logAutoScroll=this.checked}}
  var clearBtn=document.getElementById('log-clear');
  if(clearBtn)clearBtn.onclick=function(){if(el)el.innerHTML=''};

  // Fetch recent logs
  fetch('/api/logs').then(function(r){return r.json()}).then(function(entries){
    if(!el)return;
    var html='';
    (entries||[]).forEach(function(e){html+=_formatLogEntry(e)});
    el.innerHTML=html||'<div style="color:var(--fg-dim);padding:20px;text-align:center">No recent logs</div>';
    if(_logAutoScroll)el.scrollTop=el.scrollHeight;
    // Start live stream
    _connectLogStream();
  }).catch(function(e){
    if(el)el.innerHTML='<div style="color:#d9534f;padding:20px">Failed to load logs: '+esc(e.message)+'</div>';
  });
}

function _refilterLogs(){
  // Easiest: reload all recent logs with filter applied client-side
  var el=document.getElementById('log-entries');
  if(!el)return;
  el.innerHTML='<div class="loading-center"><span class="spinner"></span></div>';
  fetch('/api/logs').then(function(r){return r.json()}).then(function(entries){
    var html='';
    (entries||[]).forEach(function(e){html+=_formatLogEntry(e)});
    el.innerHTML=html||'<div style="color:var(--fg-dim);padding:20px;text-align:center">No logs at this level</div>';
    if(_logAutoScroll)el.scrollTop=el.scrollHeight;
  }).catch(function(){});
}

// ─── Page Router ────────────────────────────────────────────────────────────

function renderPage(){
  var main=document.getElementById('main');
  var html='';
  switch(S.page){
    case'browse':html=renderBrowse();break;
    case'dashboard':html=renderDashboard();break;
    case'uploads':html=renderUploads();break;
    case'search':html=renderSearch();break;
    case'tree':html=renderTree();break;
    case'metrics':html=renderMetrics();break;
    case'activity':html=renderActivity();break;
    case'logs':html=renderLogs();break;
    default:html=renderBrowse();
  }
  main.innerHTML=html+'<div class="drop-overlay"><div class="drop-overlay-icon">📂</div><div class="drop-overlay-text">Drop files or folders here to upload</div></div>';

  // Update active nav
  document.querySelectorAll('.nav-item').forEach(function(el){
    el.classList.toggle('active',el.getAttribute('data-page')===S.page);
  });
}

function goPage(page,opts){
  // Disconnect log stream when leaving logs page
  if(S.page==='logs'&&page!=='logs')_disconnectLogStream();
  S.page=page;
  try{sessionStorage.setItem('pdrive_page',page)}catch(e){}
  S.selected.clear();
  updateSelectionBar();
  closeInfoPanel();
  updateTitle();
  switch(page){
    case'browse':var bp=opts&&opts.path;if(bp)try{sessionStorage.setItem('pdrive_path',bp)}catch(e){}loadBrowse(bp);break;
    case'dashboard':loadDashboard();break;
    case'uploads':loadUploads();break;
    case'search':renderPage();setTimeout(function(){var el=document.getElementById('search-pattern');if(el)el.focus()},50);break;
    case'tree':if(!S.treeData)loadTree();else renderPage();break;
    case'metrics':loadMetrics();break;
    case'activity':loadActivity();break;
    case'logs':loadLogs();break;
    default:renderPage();
  }
}

function updateTitle(){
  var t='pdrive';
  if(S.page==='browse'&&S.path&&S.path!=='/')t='pdrive — '+S.path;
  else if(S.page!=='browse')t='pdrive — '+S.page.charAt(0).toUpperCase()+S.page.slice(1);
  document.title=t;
}

// ─── Actions ────────────────────────────────────────────────────────────────

function doDownload(p){
  var a=document.createElement('a');
  a.href=api.downloadUrl(p);
  a.download=pathBasename(p);
  a.style.display='none';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  toast('Downloading '+pathBasename(p),'success');
}

function doPin(p){
  api.pin(p).then(function(){
    toast('Pinned '+pathBasename(p),'success');
    if(S.page==='browse')loadBrowse(S.path);
  }).catch(function(e){toast('Pin failed: '+e.message,'error')});
}

function doUnpin(p){
  api.unpin(p).then(function(){
    toast('Unpinned '+pathBasename(p),'success');
    if(S.page==='browse')loadBrowse(S.path);
  }).catch(function(e){toast('Unpin failed: '+e.message,'error')});
}

function doDelete(p){
  showModal({title:'Delete',message:'Delete "'+pathBasename(p)+'"? This cannot be undone.',confirmLabel:'Delete',danger:true}).then(function(ok){
    if(!ok)return;
    api.del(p).then(function(){
      toast('Deleted '+pathBasename(p),'success');
      closeInfoPanel();
      if(S.page==='browse')loadBrowse(S.path);
    }).catch(function(e){toast('Delete failed: '+e.message,'error')});
  });
}

function doMove(p){
  showModal({title:'Move / Rename',message:'Enter new path:',input:true,inputValue:p,confirmLabel:'Move'}).then(function(result){
    if(!result)return;
    api.mv(p,result).then(function(){
      toast('Moved to '+pathBasename(result),'success');
      closeInfoPanel();
      if(S.page==='browse')loadBrowse(S.path);
    }).catch(function(e){toast('Move failed: '+e.message,'error')});
  });
}

function doNewFolder(){
  var base=S.path==='/'?'/':S.path+'/';
  showModal({title:'New Folder',message:'Folder name:',input:true,inputPlaceholder:'my-folder',confirmLabel:'Create'}).then(function(name){
    if(!name)return;
    var full=base+name;
    api.mkdir(full).then(function(){
      toast('Created '+name,'success');
      loadBrowse(S.path);
    }).catch(function(e){toast('Failed: '+e.message,'error')});
  });
}

function doDuHere(){
  api.du(S.path).then(function(d){
    toast(d.file_count+' files, '+fmtSize(d.total_bytes)+' total','info');
  }).catch(function(e){toast('Error: '+e.message,'error')});
}

var _uploadInProgress=false;
var _uploadCancelled=false;

// ── Upload Progress Drawer ─────────────────────────────────────────────────

var uploadTracker={
  items:[],       // {name,size,status:'queued'|'uploading'|'submitted'|'done'|'fail',error:null,virtualPath:null}
  minimized:false,
  visible:false,
  _closeTimer:null,

  start:function(fileList){
    // fileList: [{name,size}]
    if(this._closeTimer){clearTimeout(this._closeTimer);this._closeTimer=null;}
    this.items=fileList.map(function(f){return{name:f.name,size:f.size,status:'queued',error:null,virtualPath:null}});
    this.minimized=false;
    this.visible=true;
    this.render();
  },
  markUploading:function(idx){
    if(this.items[idx])this.items[idx].status='uploading';
    this.render();
  },
  markSubmitted:function(idx,virtualPath){
    if(this.items[idx]){this.items[idx].status='submitted';this.items[idx].virtualPath=virtualPath||null;}
    this.render();
  },
  markDone:function(idx){
    if(this.items[idx])this.items[idx].status='done';
    this.render();
  },
  markFailed:function(idx,err){
    if(this.items[idx]){this.items[idx].status='fail';this.items[idx].error=err;}
    this.render();
  },
  updateFromBackend:function(ups){
    if(!this.visible||!this.items.length)return;
    var activeSet={};
    (ups||[]).forEach(function(u){activeSet[u.VirtualPath]=true});
    var changed=false;
    this.items.forEach(function(it){
      if(it.status==='submitted'&&it.virtualPath&&!activeSet[it.virtualPath]){
        it.status='done';
        changed=true;
      }
    });
    if(changed){
      this.render();
      var c=this.counts();
      if(c.done+c.fail>=c.total)this.scheduleAutoMinimize();
    }
  },
  counts:function(){
    var d=0,f=0,q=0,u=0,s=0;
    this.items.forEach(function(it){
      if(it.status==='done')d++;
      else if(it.status==='fail')f++;
      else if(it.status==='uploading')u++;
      else if(it.status==='submitted')s++;
      else q++;
    });
    return{done:d,fail:f,queued:q,uploading:u,submitted:s,total:this.items.length};
  },
  toggle:function(){
    this.minimized=!this.minimized;
    this.render();
  },
  close:function(){
    this.visible=false;
    this.items=[];
    if(this._closeTimer){clearTimeout(this._closeTimer);this._closeTimer=null;}
    var el=document.getElementById('upload-drawer');
    el.classList.remove('visible');
  },
  scheduleAutoMinimize:function(){
    var self=this;
    this._closeTimer=setTimeout(function(){
      self.minimized=true;
      self.render();
    },3000);
  },
  render:function(){
    var el=document.getElementById('upload-drawer');
    if(!this.visible||!this.items.length){el.classList.remove('visible');el.innerHTML='';return;}
    el.classList.add('visible');
    el.classList.toggle('minimized',this.minimized);

    var c=this.counts();
    var pct=c.total>0?Math.round((c.done+c.fail)/c.total*100):0;

    // Header
    var html='<div class="upload-drawer-header" id="upload-drawer-toggle">';
    html+='<div class="upload-drawer-title">';
    if(c.uploading>0||c.queued>0){
      html+='<span class="upload-drawer-spinner"></span> Uploading…';
    } else if(c.submitted>0){
      html+='<span class="upload-drawer-spinner"></span> Processing…';
    } else if(c.fail>0){
      html+='⚠️ Upload complete with errors';
    } else {
      html+='✅ Upload complete';
    }
    html+='</div>';
    var finished=c.done+c.fail;
    html+='<span class="upload-drawer-count">'+finished+'/'+c.total+'</span>';
    html+='<div class="upload-drawer-actions">';
    if(c.uploading>0||c.queued>0)html+='<button id="upload-drawer-cancel" title="Cancel remaining">◼</button>';
    html+='<button id="upload-drawer-min" title="'+(this.minimized?'Expand':'Minimize')+'">'+(this.minimized?'▲':'▼')+'</button>';
    if(finished>=c.total&&c.submitted===0)html+='<button id="upload-drawer-close" title="Close">✕</button>';
    html+='</div></div>';

    // Progress summary bar
    html+='<div class="upload-drawer-summary">';
    var summaryText=(c.done+c.fail)+' of '+c.total+' complete';
    if(c.submitted>0)summaryText+=' · '+c.submitted+' processing';
    if(c.fail>0)summaryText+=' · '+c.fail+' failed';
    html+='<span>'+summaryText+'</span>';
    html+='<div class="upload-drawer-progress"><div class="upload-drawer-progress-fill'+(c.fail?' has-errors':'')+'" style="width:'+pct+'%"></div></div>';
    html+='</div>';

    // File list body
    html+='<div class="upload-drawer-body">';
    this.items.forEach(function(it){
      var icon='';
      if(it.status==='done')icon='<span style="color:var(--success)">✓</span>';
      else if(it.status==='fail')icon='<span style="color:var(--danger)">✗</span>';
      else if(it.status==='submitted')icon='<span class="upload-drawer-spinner" style="border-top-color:var(--success)"></span>';
      else if(it.status==='uploading')icon='<span class="upload-drawer-spinner"></span>';
      else icon='<span style="color:var(--fg3)">○</span>';

      html+='<div class="upload-drawer-item">';
      html+='<div class="upload-drawer-icon">'+icon+'</div>';
      html+='<div class="upload-drawer-fname'+(it.status==='fail'?' fail':'')+'">'+esc(it.name)+(it.error?' — '+esc(it.error):'')+'</div>';
      html+='<div class="upload-drawer-fsize">'+fmtSize(it.size)+'</div>';
      html+='</div>';
    });
    html+='</div>';

    el.innerHTML=html;

    // Wire up toggle/close
    var self=this;
    var cancelBtn=document.getElementById('upload-drawer-cancel');
    if(cancelBtn)cancelBtn.addEventListener('click',function(e){e.stopPropagation();_uploadCancelled=true;});
    var toggleBtn=document.getElementById('upload-drawer-min');
    if(toggleBtn)toggleBtn.addEventListener('click',function(e){e.stopPropagation();self.toggle()});
    var closeBtn=document.getElementById('upload-drawer-close');
    if(closeBtn)closeBtn.addEventListener('click',function(e){e.stopPropagation();self.close()});
    document.getElementById('upload-drawer-toggle').addEventListener('click',function(){self.toggle()});
  }
};

function doUploadFiles(files){
  if(!files||!files.length)return;
  if(_uploadInProgress){toast('Upload already in progress…','info');return;}
  _uploadInProgress=true;
  _uploadCancelled=false;
  var dir=S.path||'/';
  var todo=[];
  for(var i=0;i<files.length;i++){todo.push({file:files[i],dir:dir})}
  var total=todo.length,idx=0,done=0,failed=0;
  uploadTracker.start(todo.map(function(t){return{name:t.file.name,size:t.file.size}}));
  function next(){
    if(_uploadCancelled){
      for(var c=idx;c<total;c++)uploadTracker.markFailed(c,'cancelled');
      _uploadInProgress=false;
      _uploadCancelled=false;
      uploadTracker.scheduleAutoMinimize();
      if(S.page==='browse')loadBrowse(S.path);
      return;
    }
    if(idx>=total){
      _uploadInProgress=false;
      if(S.page==='browse')loadBrowse(S.path);
      return;
    }
    var cur=idx;
    idx++;
    uploadTracker.markUploading(cur);
    api.upload(todo[cur].file,todo[cur].dir).then(function(res){
      done++;
      uploadTracker.markSubmitted(cur,res&&res.path);
      next();
    }).catch(function(e){
      failed++;
      uploadTracker.markFailed(cur,e.message);
      next();
    });
  }
  next();
}

// Upload files with relative paths (from folder input or directory drop).
function doUploadWithPaths(entries){
  if(!entries||!entries.length)return;
  if(_uploadInProgress){toast('Upload already in progress…','info');return;}
  _uploadInProgress=true;
  _uploadCancelled=false;
  var baseDir=S.path||'/';
  var todo=entries.map(function(e){return{file:e.file,dir:baseDir==="/"?baseDir+e.relDir:baseDir+"/"+e.relDir}});
  var total=todo.length,idx=0,done=0,failed=0;
  uploadTracker.start(todo.map(function(t){return{name:t.file.name,size:t.file.size}}));
  function next(){
    if(_uploadCancelled){
      for(var c=idx;c<total;c++)uploadTracker.markFailed(c,'cancelled');
      _uploadInProgress=false;
      _uploadCancelled=false;
      uploadTracker.scheduleAutoMinimize();
      if(S.page==='browse')loadBrowse(S.path);
      return;
    }
    if(idx>=total){
      _uploadInProgress=false;
      if(S.page==='browse')loadBrowse(S.path);
      return;
    }
    var cur=idx;
    idx++;
    uploadTracker.markUploading(cur);
    api.upload(todo[cur].file,todo[cur].dir).then(function(res){
      done++;
      uploadTracker.markSubmitted(cur,res&&res.path);
      next();
    }).catch(function(e){
      failed++;
      uploadTracker.markFailed(cur,e.message);
      next();
    });
  }
  next();
}

// Recursively read a DataTransferItem directory entry into a flat list.
function readEntryRecursive(entry,basePath,results,done){
  if(entry.isFile){
    entry.file(function(f){
      results.push({file:f,relDir:basePath});
      done();
    },function(){done()});
  } else if(entry.isDirectory){
    var dirPath=basePath?basePath+'/'+entry.name:entry.name;
    var reader=entry.createReader();
    var allEntries=[];
    // readEntries may return partial results; call repeatedly until empty.
    (function readBatch(){
      reader.readEntries(function(batch){
        if(!batch.length){
          if(!allEntries.length){done();return;}
          var pending=allEntries.length;
          allEntries.forEach(function(child){
            readEntryRecursive(child,dirPath,results,function(){pending--;if(!pending)done();});
          });
          return;
        }
        allEntries=allEntries.concat(Array.from(batch));
        readBatch();
      },function(){done()});
    })();
  } else {done();}
}

function doVerify(p){
  toast('Verifying '+pathBasename(p)+'…','info');
  api.verify(p).then(function(res){
    if(res.ok){
      toast('✓ '+pathBasename(p)+' integrity OK','success');
    } else {
      toast('✗ '+pathBasename(p)+': '+(res.error||'failed'),'error');
    }
  }).catch(function(e){toast('Verify error: '+e.message,'error')});
}

// Bulk actions
function doBulkAction(action){
  var paths=Array.from(S.selected);
  if(!paths.length)return;

  if(action==='delete'){
    showModal({title:'Delete '+paths.length+' items',message:'This cannot be undone.',confirmLabel:'Delete All',danger:true}).then(function(ok){
      if(!ok)return;
      var chain=Promise.resolve();
      paths.forEach(function(p){chain=chain.then(function(){return api.del(p)})});
      chain.then(function(){
        toast('Deleted '+paths.length+' items','success');
        S.selected.clear();
        updateSelectionBar();
        loadBrowse(S.path);
      }).catch(function(e){toast('Error: '+e.message,'error')});
    });
    return;
  }

  var fn=action==='pin'?api.pin:action==='unpin'?api.unpin:null;
  if(fn){
    var chain=Promise.resolve();
    paths.forEach(function(p){chain=chain.then(function(){return fn(p)})});
    chain.then(function(){
      toast((action==='pin'?'Pinned':'Unpinned')+' '+paths.length+' files','success');
      S.selected.clear();
      updateSelectionBar();
      loadBrowse(S.path);
    }).catch(function(e){toast('Error: '+e.message,'error')});
    return;
  }

  if(action==='download'){
    paths.forEach(function(p){doDownload(p)});
    S.selected.clear();
    updateSelectionBar();
    return;
  }
}

// ─── Event Delegation ───────────────────────────────────────────────────────

document.addEventListener('click',function(e){
  var el=e.target.closest('[data-action]');
  if(!el)return;
  var action=el.getAttribute('data-action');
  var path=el.getAttribute('data-path');
  var page=el.getAttribute('data-page');
  var col=el.getAttribute('data-col');

  switch(action){
    case'goHome':
      e.preventDefault();
      goPage('browse',{path:'/'});
      break;
    case'goPage':
      e.preventDefault();
      goPage(page);
      break;
    case'navigate':
      e.preventDefault();
      goPage('browse',{path:path});
      break;
    case'showInfo':
      e.preventDefault();
      openInfoPanel(path);
      break;
    case'download':
      e.preventDefault();
      doDownload(path);
      break;
    case'pin':
      e.preventDefault();
      doPin(path);
      break;
    case'unpin':
      e.preventDefault();
      doUnpin(path);
      break;
    case'deleteFile':
      e.preventDefault();
      doDelete(path);
      break;
    case'moveFile':
      e.preventDefault();
      doMove(path);
      break;
    case'newFolder':
      e.preventDefault();
      doNewFolder();
      break;
    case'duHere':
      e.preventDefault();
      doDuHere();
      break;
    case'refreshBrowse':
      e.preventDefault();
      loadBrowse(S.path);
      break;
    case'uploadFile':
      e.preventDefault();
      var inp=document.getElementById('upload-input');
      if(inp)inp.click();
      break;
    case'uploadFolder':
      e.preventDefault();
      var finp=document.getElementById('upload-folder-input');
      if(finp)finp.click();
      break;
    case'verifyFile':
      e.preventDefault();
      doVerify(path);
      break;
    case'sort':
      e.preventDefault();
      if(S.sortCol===col)S.sortAsc=!S.sortAsc;
      else{S.sortCol=col;S.sortAsc=true}
      renderPage();
      break;
    case'selectFile':
      // handled below
      break;
    case'selectAll':
      // handled below
      break;
    case'doSearch':
      e.preventDefault();
      doSearch();
      break;
    case'doTree':
      e.preventDefault();
      loadTree();
      break;
    case'closePanel':
      e.preventDefault();
      closeInfoPanel();
      break;
    case'modalClose':
      e.preventDefault();
      closeModal(null);
      break;
    case'modalConfirm':
      e.preventDefault();
      var inp=document.getElementById('modal-input');
      closeModal(inp?inp.value:true);
      break;
    case'bulkPin':
      e.preventDefault();
      doBulkAction('pin');
      break;
    case'bulkUnpin':
      e.preventDefault();
      doBulkAction('unpin');
      break;
    case'bulkDelete':
      e.preventDefault();
      doBulkAction('delete');
      break;
    case'bulkDownload':
      e.preventDefault();
      doBulkAction('download');
      break;
    case'clearSelection':
      e.preventDefault();
      S.selected.clear();
      updateSelectionBar();
      renderPage();
      break;
    case'showKbdHelp':
      e.preventDefault();
      toggleKbdHelp();
      break;
    case'resyncProviders':
      e.preventDefault();
      api.resync().then(function(){
        toast('Provider resync started','success');
        setTimeout(function(){if(S.page==='dashboard')loadDashboard()},2000);
      }).catch(function(err){toast('Resync failed: '+err.message,'error')});
      break;
    case'copyText':
      e.preventDefault();
      var text=el.getAttribute('data-text');
      if(navigator.clipboard&&text){navigator.clipboard.writeText(text).then(function(){toast('Copied to clipboard','success')}).catch(function(){toast('Copy failed','error')})}
      break;
  }
});

// Checkbox handling (needs change event, not click)
document.addEventListener('change',function(e){
  var el=e.target;
  if(el.matches('[data-action="selectFile"]')){
    var p=el.getAttribute('data-path');
    if(el.checked)S.selected.add(p);else S.selected.delete(p);
    // Update row highlight
    var row=el.closest('tr');
    if(row)row.classList.toggle('selected',el.checked);
    updateSelectionBar();
  }
  if(el.matches('[data-action="selectAll"]')){
    if(!S.listing)return;
    if(el.checked){
      (S.listing.dirs||[]).forEach(function(d){S.selected.add(pathJoin(S.path,d))});
      S.listing.files.forEach(function(f){S.selected.add(f.path)});
    } else {
      S.selected.clear();
    }
    updateSelectionBar();
    renderPage();
  }
});

// Panel overlay click to close
document.getElementById('panel-overlay').addEventListener('click',function(){closeInfoPanel()});

// Modal overlay click to close
document.getElementById('modal-overlay').addEventListener('click',function(e){
  if(e.target===e.currentTarget)closeModal(null);
});

// Sidebar toggle
document.getElementById('hamburger').addEventListener('click',function(){
  document.getElementById('sidebar').classList.toggle('open');
});

// Global search
document.getElementById('global-search').addEventListener('keydown',function(e){
  if(e.key==='Enter'){
    e.preventDefault();
    var val=this.value.trim();
    if(!val)return;
    S.searchPattern=val;
    S.searchRoot='/';
    S.searchResults=null;
    goPage('search');
    setTimeout(function(){doSearch()},100);
  }
});

// ─── Keyboard Shortcuts ─────────────────────────────────────────────────────

document.addEventListener('keydown',function(e){
  var tag=document.activeElement?document.activeElement.tagName:'';

  // Modal: Escape always closes, Enter confirms (even from input)
  if(document.getElementById('modal-overlay').classList.contains('open')){
    if(e.key==='Escape'){closeModal(null);e.preventDefault();return}
    if(e.key==='Enter'){var inp=document.getElementById('modal-input');closeModal(inp?inp.value:true);e.preventDefault();return}
    return;
  }

  if(tag==='INPUT'||tag==='TEXTAREA'||tag==='SELECT')return;

  // Info panel escape
  if(document.getElementById('info-panel').classList.contains('open')){
    if(e.key==='Escape'){closeInfoPanel();e.preventDefault();return}
    return;
  }

  // Close keyboard help
  if(e.key==='Escape'&&document.getElementById('kbd-overlay').classList.contains('open')){
    toggleKbdHelp();e.preventDefault();return;
  }

  // Sidebar close on escape
  if(e.key==='Escape'){
    document.getElementById('sidebar').classList.remove('open');
    return;
  }

  // Focus search
  if(e.key==='/'){
    e.preventDefault();
    document.getElementById('global-search').focus();
    return;
  }

  // Keyboard help
  if(e.key==='?'){
    e.preventDefault();
    toggleKbdHelp();
    return;
  }

  // Browse-specific shortcuts
  if(S.page==='browse'&&S.listing){
    var allItems=[];
    (S.listing.dirs||[]).forEach(function(d){allItems.push({name:d,isDir:true,path:pathJoin(S.path,d)})});
    (sortItems(S.listing.dirs,S.listing.files).files||[]).forEach(function(f){allItems.push({name:f.name,isDir:false,path:f.path})});

    if(e.key==='j'||e.key==='ArrowDown'){
      e.preventDefault();
      if(S.focusIdx<allItems.length-1)S.focusIdx++;
      highlightFocus(allItems);
      return;
    }
    if(e.key==='k'||e.key==='ArrowUp'){
      e.preventDefault();
      if(S.focusIdx>0)S.focusIdx--;
      highlightFocus(allItems);
      return;
    }
    if((e.key==='Enter'||e.key==='l'||e.key==='ArrowRight')&&S.focusIdx>=0&&S.focusIdx<allItems.length){
      e.preventDefault();
      var item=allItems[S.focusIdx];
      if(item.isDir)goPage('browse',{path:item.path});
      else openInfoPanel(item.path);
      return;
    }
    if((e.key==='Backspace'||e.key==='h'||e.key==='ArrowLeft')&&S.path!=='/'){
      e.preventDefault();
      goPage('browse',{path:pathParent(S.path)});
      return;
    }
    if(e.key==='~'){
      e.preventDefault();
      goPage('browse',{path:'/'});
      return;
    }
    if(S.focusIdx>=0&&S.focusIdx<allItems.length&&!allItems[S.focusIdx].isDir){
      var fp=allItems[S.focusIdx].path;
      if(e.key==='d'){e.preventDefault();doDownload(fp);return}
      if(e.key==='p'){e.preventDefault();doPin(fp);return}
      if(e.key==='u'){e.preventDefault();doUnpin(fp);return}
      if(e.key==='x'){e.preventDefault();doDelete(fp);return}
      if(e.key==='i'){e.preventDefault();openInfoPanel(fp);return}
    }
    if(e.key===' '&&S.focusIdx>=0&&S.focusIdx<allItems.length){
      e.preventDefault();
      var fp=allItems[S.focusIdx].path;
      if(!allItems[S.focusIdx].isDir){
        if(S.selected.has(fp))S.selected.delete(fp);else S.selected.add(fp);
        updateSelectionBar();
        renderPage();
        highlightFocus(allItems);
      }
      return;
    }
  }
});

// ─── Keyboard Help ──────────────────────────────────────────────────────────

function toggleKbdHelp(){
  var overlay=document.getElementById('kbd-overlay');
  if(overlay.classList.contains('open')){overlay.classList.remove('open');overlay.innerHTML='';return;}
  var html='<div class="kbd-modal"><div class="kbd-modal-header"><h3>Keyboard Shortcuts</h3>'
    +'<button class="btn-icon" id="kbd-close" title="Close">✕</button></div>'
    +'<div class="kbd-modal-body">'
    +'<div class="kbd-section">Global</div>'
    +'<div class="kbd-row"><span>Search</span><span class="kbd-key">/</span></div>'
    +'<div class="kbd-row"><span>Keyboard shortcuts</span><span class="kbd-key">?</span></div>'
    +'<div class="kbd-row"><span>Close panel / sidebar</span><span class="kbd-key">Esc</span></div>'
    +'<div class="kbd-section">File Browser</div>'
    +'<div class="kbd-row"><span>Move down</span><span><span class="kbd-key">j</span> <span class="kbd-key">↓</span></span></div>'
    +'<div class="kbd-row"><span>Move up</span><span><span class="kbd-key">k</span> <span class="kbd-key">↑</span></span></div>'
    +'<div class="kbd-row"><span>Open / Enter</span><span><span class="kbd-key">Enter</span> <span class="kbd-key">l</span> <span class="kbd-key">→</span></span></div>'
    +'<div class="kbd-row"><span>Go up directory</span><span><span class="kbd-key">h</span> <span class="kbd-key">←</span> <span class="kbd-key">Backspace</span></span></div>'
    +'<div class="kbd-row"><span>Go to root</span><span class="kbd-key">~</span></div>'
    +'<div class="kbd-row"><span>Toggle select</span><span class="kbd-key">Space</span></div>'
    +'<div class="kbd-section">File Actions (when focused)</div>'
    +'<div class="kbd-row"><span>Download</span><span class="kbd-key">d</span></div>'
    +'<div class="kbd-row"><span>Pin (keep local)</span><span class="kbd-key">p</span></div>'
    +'<div class="kbd-row"><span>Unpin (evict local)</span><span class="kbd-key">u</span></div>'
    +'<div class="kbd-row"><span>Info panel</span><span class="kbd-key">i</span></div>'
    +'<div class="kbd-row"><span>Delete</span><span class="kbd-key">x</span></div>'
    +'</div></div>';
  overlay.innerHTML=html;
  overlay.classList.add('open');
  document.getElementById('kbd-close').addEventListener('click',function(){toggleKbdHelp()});
  overlay.addEventListener('click',function(ev){if(ev.target===overlay)toggleKbdHelp()});
}

function highlightFocus(allItems){
  document.querySelectorAll('.file-row.focused').forEach(function(r){r.classList.remove('focused')});
  if(S.focusIdx<0||!allItems||S.focusIdx>=allItems.length)return;
  var fp=allItems[S.focusIdx].path;
  var row=document.querySelector('.file-row[data-filepath="'+CSS.escape(fp)+'"]');
  if(row){
    row.classList.add('focused');
    row.scrollIntoView({block:'nearest'});
  }
}

// Enter in modal input
document.addEventListener('keydown',function(e){
  if(e.key==='Enter'&&document.activeElement&&document.activeElement.id==='modal-input'){
    e.preventDefault();
    closeModal(document.activeElement.value);
  }
});

// ─── Storage Bar (polling) ──────────────────────────────────────────────────

function pollStorage(){
  api.status().then(function(s){
    var used=0,total=0;
    (s.providers||[]).forEach(function(p){
      if(p.quota_total_bytes)total+=p.quota_total_bytes;
      if(p.quota_total_bytes&&p.quota_free_bytes!=null)used+=(p.quota_total_bytes-p.quota_free_bytes);
    });
    var pct=total>0?Math.min(100,used/total*100):0;
    document.getElementById('storage-fill').style.width=pct+'%';
    document.getElementById('storage-label').textContent=total>0?(fmtSize(used)+' / '+fmtSize(total)):'—';
  }).catch(function(){});
}

var _lastUploadPaths='';

function pollUploads(){
  api.uploads().then(function(ups){
    var prev=S.uploads;
    S.uploads=ups||[];
    var badge=document.getElementById('upload-badge');
    if(badge){
      if(S.uploads.length){badge.style.display='';badge.textContent=S.uploads.length}
      else{badge.style.display='none'}
    }
    // If on uploads page, refresh
    if(S.page==='uploads'){
      var el=document.getElementById('uploads-list');
      if(el)refreshUploads();
    }
    // Only refresh browse when uploads finish (transition from >0 to 0 or set changes)
    var curPaths=S.uploads.map(function(u){return u.VirtualPath}).sort().join('\n');
    if(S.page==='browse'&&_lastUploadPaths!==curPaths&&prev.length>0&&S.uploads.length<prev.length){
      loadBrowse(S.path);
    }
    _lastUploadPaths=curPaths;
    // Update the upload drawer: transition submitted→done when backend finishes
    uploadTracker.updateFromBackend(S.uploads);
  }).catch(function(){});
}

// ─── Upload: file input + drag-drop ─────────────────────────────────────────

document.addEventListener('change',function(e){
  if(e.target.id==='upload-input'){
    doUploadFiles(e.target.files);
    e.target.value='';
  }
  if(e.target.id==='upload-folder-input'){
    var files=e.target.files;
    if(!files||!files.length)return;
    var entries=[];
    for(var i=0;i<files.length;i++){
      var f=files[i];
      var rel=f.webkitRelativePath||f.name;
      var parts=rel.split('/');
      // relDir = all path components except the filename
      var relDir=parts.slice(0,parts.length-1).join('/');
      entries.push({file:f,relDir:relDir});
    }
    doUploadWithPaths(entries);
    e.target.value='';
  }
});

(function(){
  var main=document.getElementById('main');
  var dragCounter=0;
  main.addEventListener('dragenter',function(e){e.preventDefault();dragCounter++;main.classList.add('drag-over')});
  main.addEventListener('dragleave',function(e){e.preventDefault();dragCounter--;if(dragCounter<=0){dragCounter=0;main.classList.remove('drag-over')}});
  main.addEventListener('dragover',function(e){e.preventDefault()});
  main.addEventListener('drop',function(e){
    e.preventDefault();dragCounter=0;main.classList.remove('drag-over');
    if(S.page!=='browse'||!e.dataTransfer)return;
    // Collect all entries in a single pass (webkitGetAsEntry is only valid synchronously during drop).
    var items=e.dataTransfer.items;
    var entries=[];
    var hasDir=false;
    if(items){
      for(var i=0;i<items.length;i++){
        var entry=items[i].webkitGetAsEntry&&items[i].webkitGetAsEntry();
        if(entry){
          entries.push(entry);
          if(entry.isDirectory)hasDir=true;
        }
      }
    }
    if(hasDir&&entries.length){
      var results=[];var pending=entries.length;
      entries.forEach(function(entry){
        readEntryRecursive(entry,'',results,function(){pending--;if(!pending)doUploadWithPaths(results);});
      });
    } else if(e.dataTransfer.files.length){
      doUploadFiles(e.dataTransfer.files);
    }
  });
})();

// ─── Init ───────────────────────────────────────────────────────────────────

var _initPage,_initPath;
try{_initPage=sessionStorage.getItem('pdrive_page');_initPath=sessionStorage.getItem('pdrive_path')}catch(e){}
if(_initPage&&_initPage!=='browse')goPage(_initPage);
else goPage('browse',{path:_initPath||'/'});
pollStorage();
setInterval(pollStorage,15000);
setInterval(pollUploads,1000);

})();
