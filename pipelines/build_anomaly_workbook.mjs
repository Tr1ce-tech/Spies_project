import fs from 'node:fs/promises';
import path from 'node:path';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';

const runtime = createRequire(path.join(process.env.ARTIFACT_WORKDIR, 'package.json'));
const { Workbook, SpreadsheetFile } = await import(pathToFileURL(runtime.resolve('@oai/artifact-tool')));
const root = process.cwd();
const out = path.join(root, 'outputs/anomaly_review_20261022');
const data = JSON.parse(await fs.readFile(path.join(root, 'proceed_data/anomaly_workbook_v1/workbook_data.json'), 'utf8'));
const wb = Workbook.create();
const summary = wb.worksheets.add('Обзор');
for (const spec of data.sheets) wb.worksheets.add(spec.name);
const methods = wb.worksheets.add('Методика');
const col = n => {
  let s = '';
  for (n++; n; n = Math.floor((n - 1) / 26)) s = String.fromCharCode(65 + (n - 1) % 26) + s;
  return s;
};
function base(sheet) {
  sheet.showGridLines = false;
  sheet.getRange('A1:L25').format.font = {name: 'Arial', size: 10, color: '#25334A'};
  sheet.getRange('A1:L25').format.verticalAlignment = 'center';
  sheet.getRange('A1:L1').format.font = {name:'Arial',size:14,bold:true,color:'#1E3654'};
}
function header(sheet, range) {
  sheet.getRange(range).format = {fill:'#263F60',font:{name:'Arial',size:10,bold:true,color:'#FFFFFF'},wrapText:true,horizontalAlignment:'center',verticalAlignment:'center',rowHeight:42};
}
for (const [index, spec] of data.sheets.entries()) {
  const sh = wb.worksheets.getItem(spec.name);
  base(sh);
  sh.getRange('A1').values = [[spec.name]];
  sh.getRange('A2').values = [[`${spec.exported_rows.toLocaleString('ru-RU')} строк в книге; ${spec.total_rows.toLocaleString('ru-RU')} в полном наборе. Правило — на листе Методика.`]];
  const last = col(spec.headers.length-1);
  sh.getRange(`A4:${last}4`).values = [spec.headers];
  header(sh, `A4:${last}4`);
  for(let start=0; start<spec.rows.length; start+=4000) {
    const rows = spec.rows.slice(start,start+4000).map(row => row.map((v,j) => {
      if(v===null) return null;
      if(spec.date_cols.includes(j)) return new Date(v+'T00:00:00Z');
      if(typeof v==='string' && /^[=+@]/.test(v)) return "'"+v;
      return v;
    }));
    sh.getRange(`A${start+5}:${last}${start+4+rows.length}`).values=rows;
  }
  sh.getRange(`A4:${last}${Math.max(5,spec.rows.length+4)}`).format.font = {name:'Arial',size:10};
  for(let j=0;j<spec.headers.length;j++) {
    sh.getRange(`${col(j)}4:${col(j)}${Math.max(5,spec.rows.length+4)}`).format.columnWidth =
      /Причины|Проекция|Маршруты|Компонент|observation/.test(spec.headers[j]) ? 48 :
      /Источник|Статус|Единица/.test(spec.headers[j]) ? 24 : 19;
  }
  for(const j of spec.date_cols) if(spec.rows.length) sh.getRange(`${col(j)}5:${col(j)}${spec.rows.length+4}`).setNumberFormat('yyyy-mm-dd');
  for(const j of spec.text_cols) if(spec.rows.length) sh.getRange(`${col(j)}5:${col(j)}${spec.rows.length+4}`).setNumberFormat('@');
  if(spec.rows.length) {
    const table=sh.tables.add(`A4:${last}${spec.rows.length+4}`,true,`Data${index}`);
    table.showFilterButton=true;
  }
  sh.freezePanes.freezeRows(4);
  sh.freezePanes.freezeColumns(1);
  if(spec.name==='По месяцам') sh.getRange(`E5:E${spec.rows.length+4}`).setNumberFormat('0.0%');
  if(['По дням','Сигналы по датам'].includes(spec.name) && spec.rows.length) {
    sh.getRange(`E5:G${spec.rows.length+4}`).setNumberFormat('0.00');
    sh.getRange(`G5:G${spec.rows.length+4}`).conditionalFormats.addCustom('ABS(G5)>3.5',{fill:'#FCE4D6',font:{color:'#9C321D'}});
  }
  console.log(`${spec.name}: ${spec.rows.length}`);
}
base(summary);
summary.tabColor='#263F60';
summary.getRange('A1').values=[['Catch The Spies — данные для анализа']];
summary.getRange('A2').values=[['2017-01-01 — 2018-01-01. PDF исключён: расписание относится к другому периоду.']];
summary.getRange('A4:C4').values=[['Показатель','Значение','Единица / смысл']];
header(summary,'A4:C4');
const sourceEnd=data.sheets.find(x=>x.name==='Источники').rows.length+4;
const documentEnd=data.sheets.find(x=>x.name==='Документы в блоках').rows.length+4;
const dailyEnd=data.sheets.find(x=>x.name==='По дням').rows.length+4;
summary.getRange('A5:C11').values=[
  ['Первичные наблюдения',null,'Разные единицы источников'],
  ['Блоки общих поездок',data.checks.trip_blocks,'Весь набор; детали в Parquet'],
  ['Наблюдения в блоках',data.checks.trip_members,'Весь набор; детали в Parquet'],
  ['Направления по источникам',data.checks.route_rows,'Пара аэропортов внутри источника'],
  ['Необычные суточные объёмы',null,'|robust z| > 3.5'],
  ['Блоки с разными документами',null,'Проверить исходные записи'],
  ['Источников',data.sheets.find(x=>x.name==='Источники').rows.length,'PDF не включён'],
];
summary.getRange('B5').formulas=[[`=SUM('Источники'!C5:C${sourceEnd})`]];
summary.getRange('B9').formulas=[[`=COUNTIF('По дням'!G5:G${dailyEnd},">3.5")+COUNTIF('По дням'!G5:G${dailyEnd},"<-3.5")`]];
summary.getRange('B10').formulas=[[`=COUNTA('Документы в блоках'!A5:A${documentEnd})`]];
summary.getRange('B5:B11').setNumberFormat('#,##0');
summary.getRange('A4:A40').format.columnWidth=38;
summary.getRange('B4:B40').format.columnWidth=19;
summary.getRange('C4:C40').format.columnWidth=40;
summary.getRange('A14').values=[['Начать с листов «Сигналы по датам», «Документы в блоках» и «Несколько маршрутов».']];
summary.getRange('A16').values=[['Флаги описывают данные и покрытие источников. Причина отклонения требует проверки.']];
summary.getRange('A18').values=[['Детали в книге отобраны по явным правилам. Размеры полных наборов и подборок — на листе Методика.']];
summary.getRange('A20').values=[['Полные диагностические таблицы сохранены в Parquet; учёт отбора указан на листе Методика.']];
const monthly=data.sheets.find(x=>x.name==='По месяцам');
const sources=[...new Set(monthly.rows.map(r=>r[0]))];
const months=[...new Set(monthly.rows.map(r=>r[1]))].sort();
summary.getRange('A24:G24').values=[['Месяц',...sources]];
header(summary,'A24:G24');
for(let i=0;i<months.length;i++) {
  summary.getRange(`A${25+i}`).values=[[months[i].slice(0,7)]];
  summary.getRange(`B${25+i}:G${25+i}`).formulas=[sources.map(s=>{
    const idx=monthly.rows.findIndex(r=>r[0]===s&&r[1]===months[i]);
    return idx>=0 ? `='По месяцам'!E${idx+5}` : '=""';
  })];
}
summary.getRange(`B25:G${24+months.length}`).setNumberFormat('0.0%');
summary.getRange('D24:G40').format.columnWidth=15;
const chart=summary.charts.add('line',summary.getRange(`A24:G${24+months.length}`));
chart.title='Распределение записей по месяцам внутри источника';
chart.setPosition('I4','T24');
chart.titleTextStyle.typeface='Arial';chart.titleTextStyle.fontSize=12;
chart.legend={position:'bottom',textStyle:{typeface:'Arial',fontSize:10}};
chart.xAxis={axisType:'textAxis',textStyle:{typeface:'Arial',fontSize:10}};
chart.yAxis={numberFormatCode:'0%',numberFormatSourceLinked:false,textStyle:{typeface:'Arial',fontSize:10}};
const palette=['#386CB0','#C76D38','#427B68','#88649A','#9B8B3E','#738697'];
chart.series.items.forEach((s,i)=>{s.line={fill:palette[i],style:'solid',width:2};});
base(methods);methods.tabColor='#8F9BA8';
methods.getRange('A1').values=[['Методика и полнота выгрузки']];
methods.getRange('A4:D4').values=[['Лист','Строк в книге','Всего в наборе','Правило и ограничения']];
header(methods,'A4:D4');
methods.getRange(`A5:D${data.sheets.length+4}`).values=data.sheets.map(s=>[s.name,s.exported_rows,s.total_rows,s.note]);
methods.getRange(`A5:D${data.sheets.length+4}`).format.wrapText=true;
methods.getRange(`A5:D${data.sheets.length+4}`).format.rowHeight=52;
methods.getRange('A4:A45').format.columnWidth=29;
methods.getRange('B4:C45').format.columnWidth=18;
methods.getRange('D4:D45').format.columnWidth=105;
methods.freezePanes.freezeRows(4);
const notes=[
  ['Единицы','Наблюдение, профиль источника, кандидат поездки и человек — разные сущности. Числа записей не являются пассажиропотоком.'],
  ['Порог по датам','z = (число записей − медиана того же дня недели) / (1.4826 × MAD). При MAD=0 показатель отсутствует.'],
  ['Причины отклонений','Выгрузка источника, копирование, генератор данных и пропуски могут объяснять необычные объёмы. Нет независимых меток аномалий.'],
  ['Старые группы','Лист конфликтов профилей использует research_v1. Новые блоки по билетам не присваивают паспорт или личность автоматически.'],
  ['Обновление','Это снимок данных. Фильтры работают в Excel, диаграмма и сводные формулы связаны с листами. Новые Parquet требуют повторного построения.'],
  ['Доступ к деталям','Полные таблицы и ссылки на исходные записи находятся рядом с проектом. Книга содержит номера билетов; хранить локально.'],
];
methods.getRange('A19:B24').values=notes;
methods.getRange('B19:D24').merge(true);
// Place each explanation into its presentation row after merging the empty span.
for(let i=0;i<notes.length;i++) methods.getRange(`B${19+i}`).values=[[notes[i][1]]];
methods.getRange('A19:D24').format.wrapText=true;
methods.getRange('A19:D24').format.rowHeight=42;
methods.getRange('A27:D27').values=[['Лист','Источник / правило','Набор Parquet','']];
header(methods,'A27:D27');
methods.getRange(`A28:C${27+data.sheets.length}`).values=data.sheets.map(s=>[s.name,'cross-source-normalization.v1; anomaly-workbook.v1',s.parquet]);
await fs.mkdir(out,{recursive:true});
wb.recalculate();
console.log((await wb.inspect({kind:'table',range:'Обзор!A4:C11',include:'values,formulas',tableMaxRows:8,tableMaxCols:3,maxChars:2500})).ndjson);
console.log((await wb.inspect({kind:'match',searchTerm:'#REF!|#DIV/0!|#VALUE!|#NAME\\?|#NUM!|#SPILL!|#CALC!',options:{useRegex:true,maxResults:20},maxChars:1800})).ndjson);
for (const name of ['Обзор',...data.sheets.map(s=>s.name),'Методика']) {
  const blob=await wb.render({sheetName:name,range:name==='Обзор'?'A1:T24':name==='Методика'?'A1:D15':'A1:H9',scale:1,format:'png'});
  await fs.writeFile(path.join(out,`${name}.png`),new Uint8Array(await blob.arrayBuffer()));
}
const file=await SpreadsheetFile.exportXlsx(wb);
await file.save(path.join(out,'Анализ_нормализованных_данных.xlsx'));
console.log('EXPORTED');
