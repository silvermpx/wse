// Shows a WSE connection: its status and the last tick of the server's
// `ticks` topic. Every word on the screen is in German and English.
import 'package:flutter/widgets.dart';
import 'package:wse_client/wse_client.dart';

const serverUrl = String.fromEnvironment('WSE_URL', defaultValue: 'ws://127.0.0.1:5007/wse');

enum Language { de, en }

const _texts = {
  Language.de: {
    'title': 'WSE-Beispiel',
    'status': 'Verbindung',
    'connecting': 'wird aufgebaut',
    'connected': 'verbunden',
    'failed': 'fehlgeschlagen',
    'tick': 'Letzter Tick',
    'none': 'noch keiner',
    'switch': 'English',
  },
  Language.en: {
    'title': 'WSE example',
    'status': 'Connection',
    'connecting': 'connecting',
    'connected': 'connected',
    'failed': 'failed',
    'tick': 'Last tick',
    'none': 'none yet',
    'switch': 'Deutsch',
  },
};

const _ink = Color(0xFF1F2328);
const _accent = Color(0xFF0B57D0);

Future<void> main() async {
  await WseClient.init();
  runApp(const ExampleApp());
}

class ExampleApp extends StatefulWidget {
  const ExampleApp({super.key});

  @override
  State<ExampleApp> createState() => _ExampleAppState();
}

class _ExampleAppState extends State<ExampleApp> {
  Language _language = Language.de;
  String _status = 'connecting';
  String? _tick;
  WseClient? _client;

  String t(String key) => _texts[_language]![key]!;

  @override
  void initState() {
    super.initState();
    _connect();
  }

  Future<void> _connect() async {
    try {
      final client = await WseClient.connect(wseConfig(serverUrl, topics: ['ticks']));
      if (!mounted) {
        await client.close();
        return;
      }
      setState(() {
        _client = client;
        _status = 'connected';
      });
      await for (final item in client.events()) {
        if (!mounted) break;
        if (item.kind == WseItemKind.message && item.topic == 'ticks') {
          setState(() => _tick = '${item.offset}');
        }
      }
    } catch (_) {
      if (mounted) setState(() => _status = 'failed');
    }
  }

  @override
  void dispose() {
    _client?.close();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return WidgetsApp(
      title: t('title'),
      color: _accent,
      textStyle: const TextStyle(fontSize: 18, color: _ink),
      builder: (context, _) => ColoredBox(
        color: const Color(0xFFFFFFFF),
        child: SafeArea(
          child: Padding(
            padding: const EdgeInsets.all(24),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(t('title'), style: const TextStyle(fontSize: 26, fontWeight: FontWeight.w600)),
                const SizedBox(height: 24),
                Text('${t('status')}: ${t(_status)}', key: const Key('status')),
                const SizedBox(height: 8),
                Text('${t('tick')}: ${_tick ?? t('none')}', key: const Key('tick')),
                const SizedBox(height: 24),
                GestureDetector(
                  key: const Key('language'),
                  onTap: () => setState(() => _language = _language == Language.de ? Language.en : Language.de),
                  child: DecoratedBox(
                    decoration: BoxDecoration(
                      border: Border.all(color: _accent),
                      borderRadius: BorderRadius.circular(8),
                    ),
                    child: Padding(
                      padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 10),
                      child: Text(t('switch'), style: const TextStyle(color: _accent)),
                    ),
                  ),
                ),
              ],
            ),
          ),
        ),
      ),
    );
  }
}
