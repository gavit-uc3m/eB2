clear all
close all
clc

data = [86, 188;
        79, 238;
        241, 1098;
        802, 12212;
        166, 1423;
        61, 239;
        95, 235;
        52, 187;
        194, 1120;
        72, 189;
        780, 12381;
        ];

performance = data(:,1)./data(:,2);

p = polyfit(data(:,2), log(performance), 1);
x = linspace(0, 15e3, 15e3);
z = exp(p(2))*exp(x*p(1))+0.003;

figure, plot(data(:,2), performance, '*', 'Markersize', 12)
hold on, plot(x, z, 'linewidth', 2)
grid on, xlabel('Number of days processed'), ylabel('Time per day (s)')
title('Spark processing performance')
legend('Points', 'Regression')

