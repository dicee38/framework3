<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>@yield('title','Space Dashboard')</title>

  <!-- Bootstrap 5 -->
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">

  <!-- Leaflet (карта) -->
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>

  <!-- Общие стили -->
  <style>
    body { padding-top: 70px; }
    #map { height: 340px; }
    .chart-container { position: relative; height: 200px; }
  </style>

  @stack('styles')
</head>
<body>
<nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
  <div class="container">
    <a class="navbar-brand" href="{{ url('/dashboard') }}">Dashboard</a>
    <div class="collapse navbar-collapse">
      <ul class="navbar-nav me-auto mb-2 mb-lg-0">
        <li class="nav-item"><a class="nav-link" href="{{ url('/iss') }}">ISS</a></li>
        <li class="nav-item"><a class="nav-link" href="{{ url('/osdr') }}">OSDR</a></li>
      </ul>
    </div>
  </div>
</nav>

<div class="container">
  @yield('content')
</div>

<!-- JS: Bootstrap bundle -->
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>

<!-- Leaflet и Chart.js для Dashboard -->
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>

@stack('scripts')
</body>
</html>
