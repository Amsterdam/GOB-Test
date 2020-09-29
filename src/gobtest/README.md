# GOB end-2-end tests

## Organisatie in processen en jobs
De tests bestaan uit 1 hoofdproces waarin de test workflows worden aangestuurd en 33 test processen, totaal 34 processen.

Binnen elk proces worden een aantal jobs afgehandeld, in totaal betreft het 390 test jobs en 1 control-job, totaal 391 jobs.

De opdeling in processen is omdat de testen sequentieel moeten worden afgehandeld.
Een test proces wordt gestart en vervolgens wordt gewacht tot het proces is gestart en vervolgens totdat het proces is geëindigd voordat een nieuw test proces wordt gestart.

Alle 391 jobs zouden ook in 1 proces kunnen worden gestart.
Het nadeel daarvan is dat niet meer kan worden gecontroleerd of een proces is gestart.
Een proces wordt beschouwd als gestart als er tenminste 1 job actief is. Als alle jobs in 1 proces draaien dan is dat na de start van de eerste job altijd het geval. Vandaar de onderverdeling in meerdere processen.

Een proces wordt beschouwd als geeindigd als alle jobs geëindigd zijn.
Er wordt daarbij ook gekeken of er nog messages pending zijn in de notificatie of start-workflow queues.
Als dit gedurende een bepaalde tijd het geval is dan wordt er van uitgegaan dat er geen jobs meer actief zijn (of nog worden) in het proces.
Het proces wordt dan beschouwd als zijnde geëindigd.

### Proces identificatie
Alle testworkflows hebben een procesid dat begint met een random nummer. Binnen 1 e2e tests is dat nummer voor elke test gelijk.
In onderstaande procesid voorbeelden is dat aangegeven door <...>

### Test auto-id (9 processen)
- processid=<...>e2e_test..autoid.0 t/m 8

Het automatisch toekennen van id's wordt in deze test getest.
Id's worden automatisch toegekend voor attributen met een specificatie type 'autoid'.
Dit wordt onder andere toegepast voor de gebieden collecties (buurten, bouwblokken, etc).
Niet alleen moeten de id's correct worden toegewezen, maar ook wordt getest of een entity die ooit verwijderd is en weer opnieuw wordt opgevoerd van het juiste (originele) id wordt voorzien.

### Test auto-id met states (3 processen)
- processid=<...>.e2e_test..autoid_states.0 t/m 2

Deze testen garanderen een goede werking van partial en full updates op entiteiten met toestanden, in combinatie met auto-id.

### Test import-upload-apply (6 processen)
- processid=<...>.e2e_test..import_test.<operation>.0 t/m 5

De afhandeling van imports wordt hiermee getest

### Test relate (8 processen)
- processid=<...>.e2e_test..relate.<entity>.<entity>

De werking van het relate proces wordt hiermee getest.

### Test relate point-states (1 proces)
- processid=<...>.e2e_test..relate.collapsed_states

Testen voor het relateren van entities met state waarbij begin- en eind-geldigheid gelijk zijn.

#### Test van many relations (6 processen)
- processid=<...>.e2e_test..relate_multiple_allowed.step1 t/m step6

Testen van 1-n relaties en relaties tussen meerdere bronwaarden met verschillende relatie eigenschappen (1-1, 1-n)
