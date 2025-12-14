program LegacyCSV;

{$mode objfpc}{$H+}

uses
  SysUtils, DateUtils, Process;

function GetEnvDef(const name, def: string): string;
var v: string;
begin
  v := GetEnvironmentVariable(name);
  if v = '' then Exit(def) else Exit(v);
end;

function RandFloat(minV, maxV: Double): Double;
begin
  Result := minV + Random * (maxV - minV);
end;

function RandBoolText(): string;
begin
  if Random < 0.5 then Result := 'ЛОЖЬ' else Result := 'ИСТИНА';
end;

procedure GenerateAndCopy();
var
  outDir, fn, fullpath, pghost, pgport, pguser, pgpass, pgdb, copyCmd: string;
  f: TextFile;
  ts: string;
  voltage, temp: Double;
  logicBlock: string;
begin
  outDir := GetEnvDef('CSV_OUT_DIR', '/data/csv');
  ts := FormatDateTime('yyyymmdd_hhnnss', Now);
  fn := 'telemetry_' + ts + '.csv';
  fullpath := IncludeTrailingPathDelimiter(outDir) + fn;

  // Generate random telemetry
  voltage := RandFloat(3.2, 12.6);
  temp := RandFloat(-50.0, 80.0);
  logicBlock := RandBoolText();

  // Write CSV compatible with Excel
  AssignFile(f, fullpath);
  Rewrite(f);
  Writeln(f, 'timestamp,voltage,temp,logic_block,source_file');
  Writeln(f, FormatDateTime('yyyy-mm-dd hh:nn:ss', Now) + ',' +
             FormatFloat('0.00', voltage) + ',' +
             FormatFloat('0.00', temp) + ',' +
             logicBlock + ',' +
             fn);
  CloseFile(f);

  // COPY into Postgres
  pghost := GetEnvDef('PGHOST', 'db');
  pgport := GetEnvDef('PGPORT', '5432');
  pguser := GetEnvDef('PGUSER', 'monouser');
  pgpass := GetEnvDef('PGPASSWORD', 'monopass');
  pgdb   := GetEnvDef('PGDATABASE', 'monolith');

  // Use psql with COPY FROM PROGRAM for simplicity
  copyCmd := 'psql "host=' + pghost + ' port=' + pgport + ' user=' + pguser + ' dbname=' + pgdb + '" ' +
             '-c "\copy telemetry_legacy(timestamp, voltage, temp, logic_block, source_file) FROM ''' + fullpath + ''' WITH (FORMAT csv, HEADER true)"';
  SetEnvironmentVariable('PGPASSWORD', pgpass);
  fpSystem(copyCmd);
end;

var period: Integer;
begin
  Randomize;
  period := StrToIntDef(GetEnvDef('GEN_PERIOD_SEC', '300'), 300);
  while True do
  begin
    try
      GenerateAndCopy();
    except
      on E: Exception do
        WriteLn('Legacy error: ', E.Message);
    end;
    Sleep(period * 1000);
  end;
end.
