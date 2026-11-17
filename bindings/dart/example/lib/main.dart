// Shows a WSE connection: its status and the last tick of the server's
// `ticks` topic. Every word on the screen, and in the menu bar of macOS, is in
// German and English.
import 'dart:ui' show AppExitType;

import 'package:flutter/foundation.dart';
import 'package:flutter/services.dart';
import 'package:flutter/widgets.dart';
import 'package:wse_client/wse_client.dart';

const serverUrl = String.fromEnvironment(
  'WSE_URL',
  defaultValue: 'ws://127.0.0.1:5007/wse',
);

enum Language { de, en }

const _texts = {
  Language.de: {
    'title': 'WSE-Beispiel',
    'status': 'Verbindung',
    'connecting': 'wird aufgebaut',
    'connected': 'verbunden',
    'reconnecting': 'wird wiederhergestellt',
    'paused': 'pausiert',
    'closed': 'geschlossen',
    'failed': 'fehlgeschlagen',
    'tick': 'Letzter Tick',
    'none': 'noch keiner',
    'switch': 'English',
    'quit': 'WSE Demo beenden',
  },
  Language.en: {
    'title': 'WSE example',
    'status': 'Connection',
    'connecting': 'connecting',
    'connected': 'connected',
    'reconnecting': 'reconnecting',
    'paused': 'paused',
    'closed': 'closed',
    'failed': 'failed',
    'tick': 'Last tick',
    'none': 'none yet',
    'switch': 'Deutsch',
    'quit': 'Quit WSE Demo',
  },
};

/// A word of the screen in `language`.
String textOf(Language language, String key) => _texts[language]![key]!;

/// The status an event of the connection shows: it follows the connection
/// as it goes and comes back; null for an event that changes nothing of it.
String? statusOf(WseItemKind kind) => switch (kind) {
  WseItemKind.connected => 'connected',
  WseItemKind.disconnected => 'reconnecting',
  WseItemKind.paused => 'paused',
  WseItemKind.closed => 'closed',
  _ => null,
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

  String t(String key) => textOf(_language, key);

  void _switchLanguage() => setState(
    () => _language = _language == Language.de ? Language.en : Language.de,
  );

  @override
  void initState() {
    super.initState();
    _connect();
  }

  Future<void> _connect() async {
    try {
      final client = await WseClient.connect(
        // The example server speaks ws:// on this machine: development only.
        wseConfig(serverUrl, topics: ['ticks'], insecure: true),
      );
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
        final status = statusOf(item.kind);
        if (status != null) {
          setState(() => _status = status);
        } else if (item.kind == WseItemKind.message && item.topic == 'ticks') {
          setState(() => _tick = '${item.offset}');
        }
      }
      // The events end with the client.
      if (mounted) setState(() => _status = 'closed');
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
    final app = WidgetsApp(
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
                Text(
                  t('title'),
                  style: const TextStyle(
                    fontSize: 26,
                    fontWeight: FontWeight.w600,
                  ),
                ),
                const SizedBox(height: 24),
                Text('${t('status')}: ${t(_status)}', key: const Key('status')),
                const SizedBox(height: 8),
                Text(
                  '${t('tick')}: ${_tick ?? t('none')}',
                  key: const Key('tick'),
                ),
                const SizedBox(height: 24),
                GestureDetector(
                  key: const Key('language'),
                  onTap: _switchLanguage,
                  child: DecoratedBox(
                    decoration: BoxDecoration(
                      border: Border.all(color: _accent),
                      borderRadius: BorderRadius.circular(8),
                    ),
                    child: Padding(
                      padding: const EdgeInsets.symmetric(
                        horizontal: 16,
                        vertical: 10,
                      ),
                      child: Text(
                        t('switch'),
                        style: const TextStyle(color: _accent),
                      ),
                    ),
                  ),
                ),
              ],
            ),
          ),
        ),
      ),
    );
    // macOS shows a menu bar: in the app's language, as everything else.
    if (defaultTargetPlatform != TargetPlatform.macOS) return app;
    return PlatformMenuBar(
      menus: [
        PlatformMenu(
          label: 'WSE Demo',
          menus: [
            PlatformMenuItem(label: t('switch'), onSelected: _switchLanguage),
            PlatformMenuItemGroup(
              members: [
                PlatformMenuItem(
                  label: t('quit'),
                  shortcut: const SingleActivator(
                    LogicalKeyboardKey.keyQ,
                    meta: true,
                  ),
                  onSelected: () => ServicesBinding.instance.exitApplication(
                    AppExitType.required,
                  ),
                ),
              ],
            ),
          ],
        ),
      ],
      child: app,
    );
  }
}
